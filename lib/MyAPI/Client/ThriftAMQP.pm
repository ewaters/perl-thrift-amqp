package MyAPI::Client::ThriftAMQP;

=head1 NAME

MyAPI::Client::ThriftAMQP - Thrift AMQP client

=head1 SYNOPSIS

  use MyAPI::Client::ThriftAMQP;

  my $client = MyAPI::Client::ThriftAMQP->new(
      ThriftIDL => 'myapi.thrift',
  );

  my $sequence = $client->service_call_sequence(
      MyClass::Container::create->compose_message_call(
          customerId => 5001,
          name       => 'container2',
      )
  );

  $sequence->add_callback(sub {
      my ($sequence, $result) = @_;

      $client->logger->info("Successful container create: got $result");
  });

  $sequence->add_error_callback(sub {
      my ($sequence, $error) = @_;

      $client->logger->error("An error has occurred: $error");
  });

  $sequence->add_finally_callback(sub {
      $client->stop;
  });

  $client->run;

=head1 DESCRIPTION

A client interface to the Thrift AMQP implementation of an API, this library encompasses all the logic needed to compose requests to ThriftAMQP API services and handle their responses in an asynchronous manner.

=cut

use strict;
use warnings;
use base qw(MyAPI::Common::ThriftAMQP);
__PACKAGE__->mk_group_accessors(simple => qw(
    amq
    logger
    alias
));
use POE qw(
    Component::Client::AMQP
    Component::Sequence
);
use Net::AMQP::Protocol::v0_8;
use Params::Validate;
use Thrift::IDL;
use Thrift::Parser;

# AMQP debug
use YAML::XS qw();
use Term::ANSIColor qw(:constants);
use Data::Dumper;

=head1 Class Methods

=head2 new

=over

=item I<ThriftIDL> (filename, required)

=item I<Alias> (default 'api-client')

=item I<Debug> (applies to L<Thrift::IDL> parser)

=item I<Testing> (set to '1' for testing - no AMQP connection)

=item I<Logger> (defaults to AMQP logger object>

=item I<RemoteAddress>

=item I<RemotePort>

=item I<Username>

=item I<Password>

=item I<VirtualHost>

=item I<SSL> (default true)

These parameters are passed through to the L<POE::Component::Client::AMQP> creation.

=back

=cut

sub new {
    my $class = shift;

    my %opts = validate(@_, {
        ThriftIDL => 1,
        Alias     => { default => 'api-client' },
        Debug     => 0,
        Testing   => 0,
        Logger    => 0,
        Audit     => { default => 1 },

        # Opts for AMQP
        RemoteAddress  => 0,
        RemotePort     => 0,
        Username       => 0,
        Password       => 0,
        VirtualHost    => 0,
        SSL            => { default => 1 },,
        Keepalive      => 0,
        Reconnect      => 0,
    });

    my %self = (
        service_queues => {},
        logger  => $opts{Logger},
        alias => $opts{Alias},
        _do_when_created => [],
        _created => 0,

        # Necessary for Common function service_call()
        opts => \%opts,
        calls => {},
        last_call_id => 0,
    );
    my $self = bless \%self, $class;

    $self{idl} = ref $opts{ThriftIDL} ? $opts{ThriftIDL} : Thrift::IDL->parse_thrift_file($opts{ThriftIDL}, $ENV{DEBUG});
    if ($opts{Audit} && (my @audit = MyAPI::Common::ThriftAMQP->audit_idl_document($self{idl}))) {
        print "The IDL '$opts{ThriftIDL}' failed the MyAPI audit:\n";
        print " * $_\n" foreach @audit;
        exit 1;
    }

    foreach my $service (@{ $self{idl}->services }) {
        $self{service_parsers}{$service->name} =
            Thrift::Parser->new(idl => $self{idl}, service => $service->name);
    }

    return $self if $opts{Testing};

    $self{amq} = POE::Component::Client::AMQP->create(
        # Cause related sessions to have a related alias name
        Alias    => $opts{Alias} . '.amqp',
        AliasTCP => $opts{Alias} . '.amqp_tcp',
        ($opts{Debug} ? (
        Debug         => {
            logic        => 1,
            frame_input  => 1,
            frame_output => 1,
            frame_dumper => sub {
                my $output = YAML::XS::Dump(shift);
                chomp $output;
                return "\n" . BLUE . $output . RESET;
            },
        },
        ) : ()),
        # Passthrough the AMQP opts from my opt hash
        map { $_ => $opts{$_} }
        grep { defined $opts{$_} }
        qw(RemoteAddress RemotePort Username Password VirtualHost SSL Logger Keepalive Reconnect)
    );

    $self{logger} ||= $self->amq->Logger;
    $self{logger}->info("Will attempt to connect to ".$self->amq->{RemoteAddress}.":".$self->amq->{RemotePort}."");

    POE::Session->create(
        object_states => [
            $self => {
                _start       => '_start',
                service_call => 'poe_service_call',
            },
        ],
    );

    return $self;
}

=head1 Object Methods

=head2 service_call_sequence

Pass in a L<Thrift::Parser::Message> object.  Returns a L<POE::Component::Sequence> call which will be passed to L</service_call> after AQMP is ready.

=cut

sub service_call_sequence {
    my ($self, $message, $opts) = @_;
    $opts ||= {};

    my $sequence = POE::Component::Sequence->new();
    $sequence->heap_set(message => $message);

    $self->do_when_created(sub {
        $poe_kernel->call($self->{opts}{Alias}, 'service_call', $message, $sequence, $opts);
    });

    return $sequence;
}

sub do_when_created {
    my ($self, $subref) = @_;
    if ($self->{_created}) {
        $subref->();
    }
    else {
        push @{ $self->{_do_when_created} }, $subref;
    }
}

sub logger {
    my $self = shift;
    return $self->amq->Logger(@_);
}

=head2 run

Shortcut to POE::Kernel->run

=cut

sub run {
    my $self = shift;
    $poe_kernel->run();
}

=head2 stop

Sends a stop signal to the AMQP client.

=cut

sub stop {
    my $self = shift;
    $self->amq->stop;
}

=head1 POE States

=cut

sub _start {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

    $kernel->alias_set($self->{opts}{Alias});

    $self->{_created} = 1;
    foreach my $subref (@{ $self->{_do_when_created} }) {
        $subref->();
    }
}

=head2 service_call

=cut

sub poe_service_call {
    my ($self, $kernel, $message, $sequence, $call_opts) = @_[OBJECT, KERNEL, ARG0 .. $#_];

    eval {
        $self->service_call($message, $sequence, $call_opts);
    };
    if (my $ex = $@) { 
        $sequence->add_action(sub { die "Failed to call MyAPI::Client::ThriftAMQP->service_call(): $ex\n" })->run;
    }
}

1;
