package POE::Component::IndependentProcesses::Parent;

=head1 NAME

POE::Component::IndependentProcesses::Parent

=head1 DESCRIPTION

See L<POE::Component::IndependentProcesses> for usage.  While most of the behavior here is implemented via internal POE states, there are a few object method calls that may be of interest to users.

=cut

use strict;
use warnings;
use POE qw(
    Wheel::SocketFactory
    Wheel::ReadWrite
    Filter::Reference
);
use Data::Dumper;
use IO::Socket;
use POSIX ":sys_wait_h";
use base qw(Class::Accessor::Grouped);
__PACKAGE__->mk_group_accessors(simple => qw(control alias));

## Avoid accumulating dead children by a non-blocking waitpid call
$SIG{CHLD} = sub {
    my $stiff;
    while (($stiff = waitpid(-1, &WNOHANG)) > 0) {
        #print "SIG CHILD reached with $stiff\n";
    }
};

sub new {
    my ($class, $control) = @_;

    my %self = (
        alias => $control->{alias},
        control => $control,
        queue => [],
        wheels => {},
        forks => {},
        connecting => {},
    );
    my $self = bless \%self, $class;

    # Copy settings into %self from control
    $self{$_} = $control->{$_} foreach qw(
        pre_fork
        max_forks
        max_requests_per_child
        max_queue
    );

    POE::Session->create(
        object_states => [
            $self => [qw(
                _start
                _stop
                pre_fork
                create_child
                connect_to_child
                child_connect
                child_connect_error
                child_input
                child_error
                enqueue_message
                flush_queue

                child_status
            )],
        ],
    );

    return $self;
}

=head1 METHODS

=cut

sub logger {
    my $self = shift;
    return $self->control->logger(@_);
}

sub cleanup {
    my ($self) = @_;

    $poe_kernel->alarm_remove_all();
    $self->{factories} = {};
    $self->{wheels} = {};
    $self->{forks} = {};
}

sub wheel_status_change {
    my ($self, $wheel_id, $new_status) = @_;

    return if $self->control->assert_is_parent(1);

    my $wheel_details = $self->{wheels}{$wheel_id};
    $wheel_details->{status} ||= 'none';
    my $message = sprintf "Child %d status changing from '%s' to '%s'",
        $wheel_details->{child_pid}, $wheel_details->{status}, $new_status;
    $self->control->callback('child_status_change', $wheel_details->{status}, $new_status);

    if ($self->{max_requests_per_child} && $wheel_details->{status} eq 'processing' && $new_status eq 'idle') {
        $wheel_details->{processed_count}++;

        if ($wheel_details->{processed_count} >= $self->{max_requests_per_child}) {
            # Shutdown child as it's reached the max of processed requests
            $self->logger->debug(sprintf "Child %d has processed %d requests (max per child) and will now exit",
                $wheel_details->{child_pid}, $wheel_details->{processed_count}
            );
            $wheel_details->{wheel}->put({
                state  => 'shutdown',
                args   => [],
                oneway => 0,
            });
            $wheel_details->{status} = 'shutdown';

            # Create a new child straight away
            # $poe_kernel->yield('create_child');
            return;
        }
        else {
            $self->logger->debug(sprintf "Child %d has processed %d requests",
                $wheel_details->{child_pid}, $wheel_details->{processed_count}
            );
        }
    }

    $wheel_details->{status} = $new_status;

    $self->show_scoreboard($message);
}

=head2 show_scoreboard ($message)

Emits to the logger a debug message containing the current scoreboard information and optionally includes the given message.

=cut

sub show_scoreboard {
    my ($self, $message) = @_;

    my %status_count;
    $status_count{$_->{status}}++ foreach values %{ $self->{wheels} };

    $self->logger->debug(sprintf "Scoreboard: %d/%d forks, %d wheels (%s), %d/%d queue%s",
        int(keys %{ $self->{forks} }),
        $self->{max_forks},
        int(keys %{ $self->{wheels} }),
        join(', ', map { "$status_count{$_} $_" } sort keys %status_count),
        int(@{ $self->{queue} }),
        $self->{max_queue},
        ($message ? ' after ' . $message : ''),
    );
}

=head2 queue_capacity

Returns how much more capacity we have in the queue before we would reject further messages.  -1 means no limit.

=cut

sub queue_capacity {
    my ($self) = @_;

    return -1 if $self->{max_queue} == -1;

    my @fork_pids  = keys %{ $self->{forks} };
    my @wheel_ids = keys %{ $self->{wheels} };
    my @idle_wheel_ids = grep { $self->{wheels}{$_}{status} eq 'idle' } @wheel_ids;
    my @shutting_down_wheel_ids = grep { $self->{wheels}{$_}{status} eq 'shutdown' } @wheel_ids;

    # Between the fork() and socket connect, we have a certain number
    # of soon-to-be idle wheels
    my $new_fork_capacity = $self->{max_forks} - int @fork_pids;
    my $soon_to_be_idle_wheels =  int @fork_pids - int @wheel_ids;
    my $effective_queue_size = int @{ $self->{queue} } - ($soon_to_be_idle_wheels
        + int @idle_wheel_ids + $new_fork_capacity + int @shutting_down_wheel_ids);

    if (($effective_queue_size + 1) > $self->{max_queue}) {
        $self->logger->info("With new fork capacity of $new_fork_capacity, idle wheels ".int(@idle_wheel_ids).", shutting down wheels ".int(@shutting_down_wheel_ids).", soon to be idle wheels of $soon_to_be_idle_wheels, and a current queue size of ".int(@{ $self->{queue} }).", we will exceed the max queue size of $$self{max_queue} with one more request");
    }

    return -1 * $effective_queue_size;
}

sub _start {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

    $kernel->alias_set($self->alias);

    $kernel->yield('pre_fork');
}

sub pre_fork {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

    foreach (1 .. $self->{pre_fork}) {
        my $is_parent = $kernel->call($self->alias, 'create_child');
        last unless $is_parent;
    }
}

sub _stop {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

}

sub create_child {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

    return if $self->control->assert_is_parent(1);

    my @fork_pids = keys %{ $self->{forks} };
    my @shutting_down_wheel_ids = grep { $self->{wheels}{$_}{status} eq 'shutdown' } keys %{ $self->{wheels} };

    if ((int @fork_pids - int @shutting_down_wheel_ids) >= $self->{max_forks}) {
        $self->logger->error(sprintf "create_child() failed: %d forks > %d max forks", int(@fork_pids), $self->{max_forks});
        return 0;
    }

    if (my $child_pid = fork()) {
        $self->{forks}{$child_pid} = -1;
        # Give it a reasonable amount of time to first check if it's started
        $kernel->delay_add('connect_to_child', .5, $child_pid);
        $self->control->callback('parent_forked', $child_pid);
        return 1;
    }
    else {
        $self->control->setup_child();
        $self->control->callback('setup_child');
        return 0;
    }
}

sub connect_to_child {
    my ($self, $kernel, $pid) = @_[OBJECT, KERNEL, ARG0];

    return if $self->control->assert_is_parent(1);

    $self->{connecting}{$pid}++;

    my $socket_fn = $self->control->child_socket_fn($pid);
    if (! -e $socket_fn) {
        $self->logger->info("Can't yet connect to child pid $pid; will try again in .5s");
        $kernel->delay('connect_to_child', .5, $pid);
        return;
    }

    my $factory = POE::Wheel::SocketFactory->new(
        SocketDomain => PF_UNIX,
        RemoteAddress => $socket_fn,
        SuccessEvent => 'child_connect',
        FailureEvent => 'child_connect_error',
    );

    $self->{factories}{ $factory->ID } = {
        child_pid => $pid,
        wheel => $factory,
        child_socket_fn => $socket_fn,
    };
}

sub child_connect {
    my ($self, $kernel, $socket, $factory_id) = @_[OBJECT, KERNEL, ARG0, ARG3];

    return if $self->control->assert_is_parent(1);

    my $details = $self->{factories}{$factory_id};

    #$self->logger->info("Connected to child " . $details->{child_pid});

    my $wheel = POE::Wheel::ReadWrite->new(
        Handle => $socket,
        InputEvent => 'child_input',
        ErrorEvent => 'child_error',
        Filter => POE::Filter::Reference->new(),
    );

    $wheel->put({ 
        state => 'parent_connected',
        oneway => 1,
        args => [ pid => $$ ],
    });

    delete $self->{connecting}{ $details->{child_pid} };

    $self->{forks}{ $details->{child_pid} } = $wheel->ID;
    $self->{wheels}{ $wheel->ID } = {
        child_pid => $details->{child_pid},
        wheel => $wheel,
    };
    $self->wheel_status_change($wheel->ID, 'startup');
}

sub child_connect_error {
    my ($self, $kernel, $operation, $errnum, $errstr, $wheel_id) =
        @_[OBJECT, KERNEL, ARG0 .. $#_];

    my $details = $self->{factories}{$wheel_id};
    my $socket_fn = $details ? $details->{child_socket_fn} : 'unknown';
    $self->logger->error("Failed to connect to child socket $socket_fn: $operation $errnum $errstr $wheel_id");
}

sub child_input {
    my ($self, $kernel, $input, $wheel_id) = @_[OBJECT, KERNEL, ARG0, ARG1];
    #$self->logger->info("Child input on $wheel_id: ".Dumper($input));

    return if $self->control->assert_is_parent(1);

    if (! defined $input || ! ref $input || ref $input ne 'HASH'
        || ! $input->{state} || ! $input->{args} || ! ref $input->{args}
        || ref $input->{args} ne 'ARRAY') {
        $self->logger->error("Parent received an invalid input from child on wheel $wheel_id: " . Dumper($input));
        return;
    }

    # Post the message to my session

    $kernel->yield( $input->{state}, $wheel_id, $input->{oneway}, @{ $input->{args} } );
}

sub child_status {
    my ($self, $kernel, $wheel_id, $oneway, $status) = @_[OBJECT, KERNEL, ARG0 .. $#_];

    my $wheel_details = $self->{wheels}{$wheel_id};

    $self->wheel_status_change($wheel_id, $status);
    $kernel->yield('flush_queue') if $status eq 'idle';
}

sub child_error {
    my ($self, $kernel, $operation, $errnum, $errstr, $wheel_id) =
        @_[OBJECT, KERNEL, ARG0 .. $#_];

    if ($operation eq 'read' && $errnum == 0) {
        my $wheel_details = $self->{wheels}{$wheel_id};
        $self->logger->error(
            "Wheel $wheel_id (child pid $$wheel_details{child_pid}) has disconnected from "
            . $self->control->child_socket_fn( $wheel_details->{child_pid} )
            . "; has status of '$$wheel_details{status}' prior to disconnect"
        );

        # TODO: Now what? Reconnect?
        delete $self->{forks}{ $wheel_details->{child_pid} };
        delete $self->{wheels}{$wheel_id};

        # If we requested a child to shutdown (maybe due to max requests per child) and there are no more forks, fork one more
        #if ($wheel_details->{status} eq 'shutdown' && int(keys %{ $self->{forks} }) == 0) {
        #    $kernel->yield('create_child');
        #}

        return;
    }

    $self->logger->error("Error in child socket: $operation $errnum $errstr $wheel_id");
}

sub enqueue_message {
    my ($self, $kernel, $details) = @_[OBJECT, KERNEL, ARG0];

    return if $self->control->assert_is_parent(1);

    my @fork_pids  = keys %{ $self->{forks} };
    my @wheel_ids = keys %{ $self->{wheels} };
    my @idle_wheel_ids = grep { $self->{wheels}{$_}{status} eq 'idle' } @wheel_ids;
    my @shutting_down_wheel_ids = grep { $self->{wheels}{$_}{status} eq 'shutdown' } @wheel_ids;

    if ($self->{max_queue} > -1) {
        my $queue_size = -1 * $self->queue_capacity;
        if (($queue_size + 1) > $self->{max_queue}) {
            # We have no more capacity; fail the request
            return 0;
        }
    }

    # Enqueue the request
    push @{ $self->{queue} }, $details;

    $self->show_scoreboard("Enqueued request");

    if (int @idle_wheel_ids < int @{ $self->{queue} }
        && (int @fork_pids - int @shutting_down_wheel_ids) < $self->{max_forks}) {
        $self->logger->info("Creating new child for demand");
        $kernel->yield('create_child');
    }
    else {
        $kernel->yield('flush_queue');
    }
    
    return 1;
}

sub flush_queue {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

    return if $self->control->assert_is_parent(1);

    my @fork_pids  = keys %{ $self->{forks} };
    my @wheel_ids = keys %{ $self->{wheels} };
    while (int @{ $self->{queue} }) {
        my @idle_wheel_ids = grep { $self->{wheels}{$_}{status} eq 'idle' } @wheel_ids;
        last unless @idle_wheel_ids;

        # Get a random idle wheel id
        my $wheel_id = $idle_wheel_ids[ int rand int @idle_wheel_ids ];

        $self->logger->debug("Handing next request to wheel $wheel_id");
        $self->{wheels}{$wheel_id}{wheel}->put(shift @{ $self->{queue} });
        $self->wheel_status_change($wheel_id, 'processing');
    }
    #$self->logger->debug("After flush_queue() we have ".int(@{ $self->{queue} })." message(s) in queue");
}

1;
