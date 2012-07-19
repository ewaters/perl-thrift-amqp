package POE::Component::IndependentProcesses;

=head1 NAME

POE::Component::IndependentProcesses

=head1 SYNOPSIS

  use POE qw(Component::IndependentProcesses);

  my $pm;
  $pm = POE::Component::IndependentProcesses->new(
      logger    => $logger,
      max_queue => 0,
      callbacks => {
          parent_forked => sub {
              my ($child_pid) = @_;
              $logger->info("Forked to $child_pid");
          },
          start_child   => sub {
              $logger->info("start_child");
          },
          child_status_change => sub {
              my ($old_status, $new_status) = @_;
              $logger->info("Child changed from '$old_status' to '$new_status'");
          },
      },
      child_methods => {
          do_something => sub {
              my ($oneway, %args) = @_;
              $logger->info("do something $args{count}!");
              $pm->update_status('idle');
          },
      },
  );
                      
  POE::Session->create(
      inline_states => {
          _start => sub {
              my ($kernel, $heap) = @_[KERNEL, HEAP];
              $kernel->yield('next_task');
          },               
          next_task => sub {
              my ($kernel, $heap) = @_[KERNEL, HEAP];
              
              # Since we're forking, this session and it's methods will fork too into each child
              return if $pm->assert_is_parent(1);
              
              print "\n\n";
              
              $logger->debug("currently have queue capacity of " . $pm->parent->queue_capacity);
              $pm->call_child_method(
                  method => 'do_something',
                  args => [
                      count => $heap->{count}++,
                  ],
                  oneway => 0,
              );
              
              $kernel->delay('next_task', 1);
          },
      },
  );

  $poe_kernel->run();

=head1 DESCRIPTION

This module implements a prefork worker model for parallel method calls.  The parent process uses C<fork()> to create child processes and uses L<POE::Filter::Reference> and L<POE::Wheel::ReadWrite> to communicate with them over bi-directional sockets, one per child.  The parent maintains a scoreboard which indicates the status of all child workers as well as maintains an optional queue of pending messages that need to be delivered to the children, as they become available.

All logic for configuring the worker system is provided in C<new()> below.

When a new child is created, the following happens:

=over 4

=item * Parent forks

=item * Child cleans up POE state that it inherited from the parent and creates an instance of L<POE::Component::IndependentProcesses::Child> which takes over control

=item * Child creates socket file and listens for the parent to connect

=item * Parent, which was waiting for the socket file to come into existance, connects to socket file and informs the child 'parent_connected'.  It marks the status of the child as 'startup'

=item * Child receives 'parent_connected' message and responds by indicating it's status is 'idle', which the Parent records

=back

Here's how a method call is made to the child:

=over 4

=item * Parent chooses a Child that is status 'idle', sends the method call message (whatever it may contain), and marks the status of the child as being 'processing'

=item * The child must report back after it has completed the request with status 'idle'

=back

Since there is no defined way at this point to send arbitrary messages or method responses back to the Parent, this can be used as an RPC system only if the child knows how to communicate a response back to the requestor.  This was designed to handle forking and message dispatching for an AMQP RPC system where the child maintains its own connection to the AMQP server and has the ability to make further RPC calls as needed to complete it's own requests, and is able to send the method response directly to the requestor using this AMQP connection, thereby removing any need to talk to the parent aside from to indicate that it's 'idle' when it's done.

The design is intended to be extendable such that a graceful restart of the parent is possible.  Since the children maintain independent connections to the parent via their own socket files, it's possible for the old parent to disappear and a new parent to take control of the processes.  Children may also have multiple parents, although this has not been fully worked out.

=cut

use strict;
use warnings;
use POE qw(
    Component::IndependentProcesses::Parent
    Component::IndependentProcesses::Child
);
use Data::Dumper;
use Params::Validate qw(validate ARRAYREF);
use base qw(Class::Accessor::Grouped);
__PACKAGE__->mk_group_accessors(simple => qw(logger parent child));

=head1 CLASS METHODS

=head2 new (%params)

Takes the following parameters:

=over 4

=item logger (required)

L<Log::Log4perl> style object for logging

=item alias

Speicfy a POE session alias.  Default: 'process_manager'

=item pre_fork

Indicate a number of forks to start out with.  Default: 0

=item max_forks

Maximum number of forks to spawn under load.  Default: 4

=item max_requests_per_child

Maximum number of requests to handle per fork before killing the fork and allowing it to respawn as need demands.  Default: 0 (no max)

=item max_queue

Allow a certain number of requests to queue up without an active child to handle it.  Default: -1 (no queueing will happen)

=item callbacks (required)

Hashref of named callbacks that may be called in different contexts:

=over 4

=item start_child

Called with no arguments by the child process after it starts (creates listening socket for parent to connect to)

=item parent_forked ($pid)

Called (by the parent) with the pid of the new child process after a new child has forked.

=item setup_child

Called by the child after it has been forked

=item child_status_change ($old_status, $new_status)

Called (by the parent) after it's received a status change from a child

=back 4

=item child_methods (required)

Hashref of named methods that can be called via the parent method C<call_child_method>.  The signature of each method is simply: ($oneway, @args).  See C<call_child_method> for more details.

=back 4

=cut

sub new {
    my $class = shift;

    my %self = validate(@_, {
        logger       => 1,
        alias        => { default => 'process_manager' },
        pre_fork     => { default => 0 },
        max_forks    => { default => 4 },
        max_requests_per_child => { default => 0 },
        max_queue    => { default => -1 },
        callbacks    => 1,
        child_methods => 1,
        socket_base_fn => { default => '/var/tmp/poe_component_independentprocesses' },
    });
    my $self = bless \%self, $class;

    $self->{socket_fns} = [];
    $self->{parent_pid} = $$;
    $self->{parent} = POE::Component::IndependentProcesses::Parent->new($self);
    $self->{child}  = undef;

    return $self;
}

=head1 PARENT METHODS

=head2 call_child_method (%params)

Enqueue a message to be handled as soon a possible, according to fork and queueing settings, by a child process.

=over 4

=item method (required)

Name the method (found in the hash 'child_methods') that will be called on the child.

=item args

Optional array reference of data that will be passed to the child method subroutine.

=item oneway

Does nothing at the moment.

=cut

sub call_child_method {
    my $self = shift;
    return if $self->assert_is_parent(1);

    my %args = validate(@_, {
        method => 1,
        args => { default => [], type => ARRAYREF },
        oneway => { default => 0 },
    });
    
    $poe_kernel->call($self->parent->alias, 'enqueue_message', {
        state  => 'user_callback',
        args   => [ $args{method}, @{ $args{args} } ],
        oneway => $args{oneway},
    });
}

=head1 CHILD METHODS

=head2 update_status ($status)

Updates parent with new status $status for this child

=cut

sub update_status {
    my ($self, $status) = @_;
    return if $self->assert_is_parent(0);

    $poe_kernel->call($self->child->alias, 'change_status', $status);
}

=head2 setup_child

=cut

sub setup_child {
    my $self = shift;

    return if $self->assert_is_parent(0);

    $self->parent->cleanup;
    delete $self->{parent};

    $self->{child} = POE::Component::IndependentProcesses::Child->new($self);
}

=head2 child_socket_fn

Return a filename (based on 'socket_base_fn' C<new> arg) for this pid for usage as a socket.  These will be cleared up in C<DESTROY>.

=cut

sub child_socket_fn {
    my ($self, $pid) = @_;
    my $socket_fn = $self->{socket_base_fn} . "-$pid.sock";
    push @{ $self->{socket_fns} }, $socket_fn;
    return $socket_fn;
}

=head1 METHODS

=head2 assert_is_parent ($should_be_parent)

Return boolean if assertion $should_be_parent is true with what we currently are.

=cut

sub assert_is_parent {
    my ($self, $should_be_parent)  = @_;

    my ($package, $filename, $line, $subroutine, @other) = caller(1);

    my $is_parent = $self->{parent_pid} == $$ ? 1 : 0;

    if ($should_be_parent == $is_parent) {
        return 0;
    }

    $self->logger->error("Can't call $subroutine when ".($should_be_parent ? 'not' : '')." parent");
    return 1;
}

=head2 callback ($name, @args)

Call the user-specificed callback $name with @args

=cut

sub callback {
    my ($self, $callback, @args) = @_;

    my $subref = $self->{callbacks}{$callback};
    return unless $subref;

    $subref->(@args);
}

sub DESTROY {
    my $self = shift;
    foreach my $fn (@{ $self->{socket_fns} }) {
        next unless -e $fn;
        unlink $fn;
    }
}

1;
