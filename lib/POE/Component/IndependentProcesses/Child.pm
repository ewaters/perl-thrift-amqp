package POE::Component::IndependentProcesses::Child;

=head1 NAME

POE::Component::IndependentProcesses::Child

=head1 DESCRIPTION

See L<POE::Component::IndependentProcesses> for usage.  This class is not used directly, and almost exclusively has POE states for method calls.

=cut

use strict;
use warnings;
use POE qw(
    Wheel::SocketFactory
    Wheel::ReadWrite
    Filter::Reference
);
use IO::Socket;
use base qw(Class::Accessor::Grouped);
__PACKAGE__->mk_group_accessors(simple => qw(control alias));

sub new {
    my ($class, $control) = @_;

    my %self = (
        control => $control,
        alias => $control->{alias} . '_' . $$,
        wheels => {},
        status => 'idle',
    );
    my $self = bless \%self, $class;

    POE::Session->create(
        object_states => [
            $self => [qw(
                _start
                _stop
                got_client
                got_error
                got_client_input
                got_client_error
                server_send

                parent_connected
                user_callback

                change_status
                shutdown
            )],
        ],
    );

    return $self;
}

sub logger {
    my $self = shift;
    return $self->control->logger(@_);
}

sub _start {
    my ($self, $kernel, $heap) = @_[OBJECT, KERNEL, HEAP];

    $kernel->alias_set($self->alias);

    $self->{socket_fn} = $self->control->child_socket_fn($$);
    unlink $self->{socket_fn} if -e $self->{socket_fn};

    # Create listening socket for parent to connect to

    $self->{server} = POE::Wheel::SocketFactory->new(
        SocketDomain => PF_UNIX,
        BindAddress  => $self->{socket_fn},
        SuccessEvent => 'got_client',
        FailureEvent => 'got_error',
    );

    $self->control->callback('start_child');
}

sub _stop {
    my ($self, $kernel, $heap) = @_[OBJECT, KERNEL, HEAP];

    unlink $self->{socket_fn};
}

sub DESTROY {
    my $self = shift;
    unlink $self->{socket_fn};
}

sub got_client {
    my ($self, $kernel, $heap, $socket) = @_[OBJECT, KERNEL, HEAP, ARG0];

    #$self->logger->info("Connected to parent process manager");

    my $wheel = POE::Wheel::ReadWrite->new(
        Handle => $socket,
        InputEvent => 'got_client_input',
        ErrorEvent => 'got_client_error',
        Filter     => POE::Filter::Reference->new(),
    );
    $self->{wheels}{ $wheel->ID } = $wheel;
}

sub got_error {
    my ($self, $kernel, $heap, $operation, $errnum, $errstr, $wheel_id) =
        @_[OBJECT, KERNEL, HEAP, ARG0 .. $#_];
    $self->logger->error("Error in socket factory: $operation $errnum $errstr $wheel_id");
}

sub got_client_input {
    my ($self, $kernel, $heap, $input, $wheel_id) = @_[OBJECT, KERNEL, HEAP, ARG0, ARG1];

    if (! defined $input || ! ref $input || ref $input ne 'HASH'
        || ! $input->{state} || ! $input->{args} || ! ref $input->{args}
        || ref $input->{args} ne 'ARRAY') {
        $self->logger->error("Child received an invalid input from parent on wheel $wheel_id");
        # TODO: report back to parent  
        return;
    }

    # Post the message to my session

    $kernel->yield( $input->{state}, $wheel_id, $input->{oneway}, @{ $input->{args} } );
}

sub got_client_error {
    my ($self, $kernel, $heap, $operation, $errnum, $errstr, $wheel_id) =
        @_[OBJECT, KERNEL, HEAP, ARG0 .. $#_];

    if ($operation eq 'read' && $errnum == 0) {
        $self->logger->error("Parent wheel $wheel_id has disconnected from " . $self->{socket_fn});
        # TODO: Now what? Reconnect?
        delete $self->{wheels}{$wheel_id};

        my $remaining = int keys %{ $self->{wheels} };
        if (! $remaining) {
            $self->logger->error("No other 'Parent' processes are connected to my socket; I'm going to exit");
            exit;
        }
        else {
            $self->logger->info("Currently have $remaining 'Parent's connected to me");
        }

        return;
    }
    $self->logger->error("Error in socket: $operation $errnum $errstr $wheel_id");
}

sub server_send  {
    my ($self, $kernel, $heap, $message) = @_[OBJECT, KERNEL, HEAP, ARG0];

    foreach my $wheel (values %{ $self->{wheels} }) {
        $wheel->put($message);
    }
}

sub parent_connected {
    my ($self, $kernel, $wheel_id) = @_[OBJECT, KERNEL, ARG0];

    $self->{wheels}{$wheel_id}->put({
        state => 'child_status',
        args => [ $self->{status} ],
    });
}

sub user_callback {
    my ($self, $kernel, $wheel_id, $oneway, $method, @args) = @_[OBJECT, KERNEL, ARG0 .. $#_];

    my $code = $self->control->{child_methods}{$method};
    if (! $code) {
        # TODO: report error to parent
        $self->logger->error("User callback '$method' was not found");
        return;
    }

    $code->($oneway, @args);
}

sub change_status {
    my ($self, $kernel, $status) = @_[OBJECT, KERNEL, ARG0];

    $self->{status} = $status;
    $kernel->yield(server_send => {
        state => 'child_status',
        args => [ $status ],
    });
}

sub shutdown {
    my ($self, $kernel) = @_[OBJECT, KERNEL];
    $self->logger->error("Shutdown was requested; exiting");
    exit;
}
1;
