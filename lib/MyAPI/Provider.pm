package MyAPI::Provider;

=head1 NAME

MyAPI::Provider - Base class for API service providers

=head1 SYNOPSIS

  package MyServiceProvider;

  use base qw(MyAPI::Provider);

  sub method_ping {
      my ($self, $request) = @_;

      $request->set_result('pong');
  }

  package main;

  my $server = MyServiceProvider->new(
      log_screen => 1,
      log_debug  => 1,

      service    => 'PingPong',
      thrift_idl => 'pingpong.thrift',
      dsn_ro     => 'dbi:MySQL:host=localhost;user=ping;password=pong',
  );

  $server->run();

=head1 DESCRIPTION

This is an easy to use base class for creating a L<MyAPI::Server::ThriftAMQP> API server.  

=head1 USAGE

The server creation sets up a L<MyAPI::Server::ThriftAMQP::Handler::Object> handler with the following options:

  object => $self,
  method_prefix => 'method_',
  precall => [qw(
      precall_construct_objects
      precall_validate
  )],
  throw_on_missing_method => 1,

This means that in your derived class, you'll want to create methods that are named 'method_*' where '*' is the method call in the Thrift IDL file.  You can optionally create the methods 'precall_construct_objects' and 'precall_validate', inserting values into the request heap:

  sub precall_validate {
      my ($self, $request) = @_;

      $request->heap_set( AllIsWell => 1 );
  }

Just as in a method call, the request object controls flow, so you must eventually call $request->set_(result|error|exception) to finalize the API request.

=cut

use strict;
use warnings;
use Log::Log4perl qw(get_logger :levels);
use Log::Log4perl::Appender;
use Log::Log4perl::Layout;
use Log::Dispatch::Syslog;
use Params::Validate;
use base qw(Class::Accessor::Grouped);
__PACKAGE__->mk_group_accessors(simple => qw(logger server));
use Sys::Hostname qw(hostname);
use JSON::XS;
use Net::Address::IP::Local;

use MyAPI::Server::ThriftAMQP;

use Data::Dumper;
use Scalar::Util qw(blessed);

my ($singleton, %singleton_config, %class_data);
my $jsonxs = JSON::XS->new->utf8->allow_nonref;

my %ignore_apilog_in_methods = map { $_ => 1 } qw(ping healthCheck);

sub new {
    my $class = shift;

    my %config = validate(@_, {
        log_screen => 0,
        log_syslog => 0,
        log_file   => 0,
        log_debug  => 0,
        log_screen_debug => 0,
        log_syslog_debug => 0,
        log_file_debug   => 0,

        service    => 1,
        thrift_idl => { callbacks => { 'file exists' => sub { -e shift } } },
        virtual_host => 0,
        username   => 0,
        password   => 0,
        remote_address => 0,
        ssl        => 0,
        queue_name => 0,
        consume    => 0,
        local_ip_address => 0,

        pre_fork     => { default => 2 },
        max_forks    => { default => 8 },
        max_requests => { default => 100 },

        $class->new_spec(),
    });

    my $self = bless {
        config => \%config,
        create_time => time,
        hostname => hostname(),
    }, $class;

    $self->create_logger();

    if ($ENV{DUMP_INC_VERSIONS}) {
        my @versions;
        my $has_cpan;
        while (my ($fn, $path) = each %INC) {
            my $class = $fn;
            $class =~ s{/}{::}g;
            $class =~ s{\.pm$}{};
            next if $class =~ m{::auto::};

            my $version = eval "\$${class}::VERSION";
            $version = '(unknown)' if ! defined $version;

            my $is_cpan = $path =~ m{^/home/users/e/ewaters/perl} ? 1 : 0;
            $has_cpan = 1 if $is_cpan;

            push @versions, sprintf "%-50s %-10s %-6s %s\n", $class, $version, ($is_cpan ? 'CPAN' : 'System'), $path;
        }
        open my $out, '>', '/tmp/provider' . ($has_cpan ? '_cpan' : '_system') or die "Can't open for writing: $!";
        foreach my $line (sort @versions) {
            print $out $line;
        }
        close $out;
    }

    $self->{server} = MyAPI->create_amqp_thrift_server(
        Debug     => ($ENV{DEBUG} ? 1 : 0),
        Logger    => $self->{logger},

        Fork      => 1,
        PreFork   => $config{pre_fork},
        MaxForks  => $config{max_forks},
        MaxRequests  => $config{max_requests},

        ThriftIDL => $config{thrift_idl},
        Service   => $config{service},
        VirtualHost => $config{virtual_host},
        Username  => $config{username},
        Password  => $config{password},
        (defined $config{remote_address} ? (
        RemoteAddress => $config{remote_address},
        ) : ()),
        (defined $config{ssl} ? (
        SSL => $config{ssl},
        ) : ()),
        (defined $config{queue_name} ? (
        QueueName => $config{queue_name},
        ) : ()),

        Handlers  => [{
            object => $self,
            method_prefix => 'method_',
            precall => [qw(
                precall_create_apilog
                precall_construct_objects
                precall_validate
            )],
            on_complete => [qw(
                completed_request
            )],
            throw_on_missing_method => 1,
        }],

        (defined $config{consume} ? (
        Consume => $config{consume},
        Fork => 0,
        ) : ()),
    );

    if (my $amqp_address = $self->{server}{amq}{current_RemoteAddress}) {
        $config{local_ip_address} = Net::Address::IP::Local->connected_to($amqp_address);
        #$self->logger->debug("Determined my local IP address to be $config{local_ip_address}");
    }

    return $self;
}

sub new_spec {
    # Override in children
}

=head2 config

=head2 log

=head2 run

=over 4

Compatibility for 'bin/init' singleton approach to server starting

=back

=cut

sub config {
    my $class_or_self = shift;
    if (ref $class_or_self) {
        return $class_or_self->{config};
    }
    return \%singleton_config;
}

sub log {
    my $class_or_self = shift;
    if (ref $class_or_self) {
        return $class_or_self->logger;
    }
    else {
        $singleton ||= $class_or_self->new(%singleton_config);
        return $singleton->logger;
    }
}

sub run {
    my $self;

    # bin/init doesn't call new(), it just calls $class->run(\%run_config)
    if (int @_ == 2 && ! ref $_[0] && ref $_[1]) {
        my ($class, $run_config) = @_;
        #$class->config->{$_} = $run_config->{$_} foreach keys %$run_config;

        $singleton ||= $class->new(%singleton_config);
        $self = $singleton;
    }
    else {
        $self = shift;
    }
    
    $self->server->run;
}

=head2 report_method_status

=cut

sub report_method_status {
    my ($self, $request, $status) = @_;

    $self->logger->debug('report_method_status: ' . $status);
    my $headers = $request->headers;
    my $status_queue = $headers->{'Request-Status-Queue'};
    return unless $status_queue;

    # Clone the request headers for this status response so the user can embed contextual data
    my %response_headers = %$headers;
    delete $response_headers{$_} foreach @MyAPI::Server::reserved_headers;

    $response_headers{'API-Method-Name'} = $request->method->name;
    $response_headers{'API-Service-Name'} = $request->service->name;

    # Call server_send() directly on the AMQP Client without yielding/queueing
    POE::Kernel->call($self->server->amq->{Alias}, server_send =>
        # Ensure that each frame has the current channel id on it
        map { $_->channel( $self->server->{channel}->id ); $_ }
        map { $_->isa("Net::AMQP::Protocol::Base") ? $_->frame_wrap : $_ }
        $self->server->amq->compose_basic_publish(
            $jsonxs->encode($status),

            # Net::AMQP::Protocol::Basic::Publish
            ticket => 0,
            routing_key => $status_queue,
            mandatory => 1,

            # Net::AMQP::Frame::Header
            content_type => 'application/json',
            weight => 0,
            delivery_mode => 1,
            priority => 1,
            headers => \%response_headers,
        )
    );
}

=head2 service_call

=cut

sub service_call {
    my $self = shift;
    my %param = validate(@_, {
        request => 1,
        message => 1,
        callback => 0,
        error_callback => 0,
        call_opts => { default => {} },
        timeout => 0,
        parent_sequence => 0,
    });

    $param{parent_sequence} ||= $param{request};

    if (my $parent_status_queue = $param{request}->header('Request-Status-Queue')) {
        $param{call_opts}{headers}{'Request-Status-Queue'} = $parent_status_queue;
    }
    if ($param{timeout}) {
        $param{call_opts}{timeout} = $param{timeout};
    }
    if (my $APILog = $param{request}->heap_index('APILog')) {
        $param{call_opts}{headers}{'Request-APILog-Id'} = $APILog->id;
    }

    $param{call_opts}{headers}{'APIProxy-Shared-Secret'} = $self->config->{api_proxy_shared_secret};
    # Pass-through the ip address (as 'Source')
    if (my $ip_address = $param{request}->authentication->{ip_address}) {
        $param{call_opts}{headers}{'APIProxy-Remote-Source-IP'} = $ip_address;
    }
    # Include our local IP address as the actual remote IP
    if ($self->{config}{local_ip_address}) {
        $param{call_opts}{headers}{'APIProxy-Remote-IP'} = $self->{config}{local_ip_address};
    }

    $param{error_callback} ||= sub {
        my ($sequence, $error) = @_;
        $param{parent_sequence}->failed($error);
    };

    my $sequence = POE::Component::Sequence->new()
        ->add_finally_callback(sub {
            $param{parent_sequence}->resume();
        })
        ->add_error_callback($param{error_callback});

    if ($param{callback}) {
        $sequence->add_callback($param{callback});
    }

    # Perform the service call in the Thrift client and pause the current request
    $self->server->service_call($param{message}, $sequence, $param{call_opts});
    $param{parent_sequence}->pause();

    return;
}

=head2 _execute_pending_messages()

Called with an arrayref of composed thrift message calls

=cut
sub _execute_pending_messages {
    my ($self, $request, $messages, $call_opts) = @_;
    return unless $messages;
    $call_opts ||= {};

    if (my $parent_status_queue = $request->header('Request-Status-Queue')) {
        $call_opts->{headers}{'Request-Status-Queue'} = $parent_status_queue;
    }
    if (my $APILog = $request->heap_index('APILog')) {
        $call_opts->{headers}{'Request-APILog-Id'} = $APILog->id;
    }

    my $parent_sequence = $call_opts->{parent_sequence} || $request;

    my @pending_messages = @$messages;

    ## Process pending messages
    my (%pending_messages, @sequence_error);
    for (my $i = 0; $i <= $#pending_messages; $i++) {
        $pending_messages{$i} = 'hi';

        $self->logger->info(__PACKAGE__ . ": making service_call [$i of $#pending_messages] in _execute_pending_messages.");
        $self->server->service_call(
            $pending_messages[$i],

            POE::Component::Sequence->new()
                ->add_finally_callback(sub {
                    delete $pending_messages{$i};
                    if (int keys(%pending_messages) == 0) {
                        if (@sequence_error) {
                            $parent_sequence->failed( join('\n', @sequence_error) );
                        }
                    }
                    $parent_sequence->resume;
                })
                ->add_error_callback(sub {
                    my ($sequence, $error) = @_;
                    push @sequence_error, $error;
                }),

            $call_opts,
        );

        $parent_sequence->pause();
    }

    return;
}

=head2 create_logger ()

=cut

sub create_logger {
    my $class = shift;
    my %config = %{ $class->config };

    if (! grep { defined $config{"log_$_"} } qw(screen syslog file)) {
        print STDERR "No log destination found in config ('log_screen', 'log_syslog' or 'log_file').  Defaulting to 'log_screen'\n";
        $config{log_screen} = 1;
    }

    my $logger = Log::Log4perl->get_logger($class);
    $logger->level($DEBUG);

    my %data = ( logger => $logger );

    if ($config{log_screen}) {
        my $appender = Log::Log4perl::Appender->new(
            'Log::Log4perl::Appender::Screen',
            name => 'screenlog',
            stderr => 1,
        );
        $appender->layout( Log::Log4perl::Layout::PatternLayout->new("[\%d] \%P \%p: \%m\%n") );
        $appender->threshold($config{log_debug} || $config{log_screen_debug} ? $DEBUG : $INFO);
        $logger->add_appender($appender);
    }
    if ($config{log_syslog}) {
        my $appender = Log::Log4perl::Appender->new(
            'Log::Dispatch::Syslog',
            name => 'syslog',
            ident => $class,
            logopt => 'pid',
            min_level => 'info',
            facility => 'local4',
        );
        $appender->layout( Log::Log4perl::Layout::PatternLayout->new("\%m\%n") );
        $appender->threshold($config{log_debug} || $config{log_syslog_debug} ? $DEBUG : $INFO);
        $logger->add_appender($appender);
    }
    if ($config{log_file}) {
        my $appender = Log::Log4perl::Appender->new(
            'Log::Log4perl::Appender::File',
            name => 'filelog',
            mode => 'append',
            filename => $config{log_file},
        );
        $appender->layout( Log::Log4perl::Layout::PatternLayout->new("[\%d] \%P \%p: \%m\%n") );
        $appender->threshold($config{log_debug} || $config{log_file_debug} ? $DEBUG : $INFO);
        $logger->add_appender($appender);
    }

    $class->logger($logger);
}

sub precall_create_apilog {
    my ($self, $request) = @_;

    if ($ignore_apilog_in_methods{ $request->method->name }) {
        return;
    }

    if (! $request->transport) {
        # Local request
        return;
    }

    my $schema = $self->schema_rw;

    # Fetch the APIProviderMethod in a locked transaction, as we've seen issues where
    # two providers in different threads may try and create the new row at the same time.
    my $APIProviderMethod;
    $schema->storage->dbh_do(sub {
        my ($storage, $dbh) = @_;

        $dbh->do('LOCK TABLES APIProviderMethod AS me WRITE, APIProviderMethod WRITE');
        # find uses 'me', create uses 'APIProviderMethod'

        my %param = (
            host    => $self->{hostname},
            service => $self->{config}{service},
            method  => $request->method->name,
            vhost   => $self->{server}{amq}{VirtualHost},
        );

        $APIProviderMethod = $schema->resultset('APIProviderMethod')->find(\%param);
        if (! $APIProviderMethod) {
            $APIProviderMethod = $schema->resultset('APIProviderMethod')->create(\%param);
        }

        $dbh->do('UNLOCK TABLES');
    });

    my $APILog = $schema->resultset('APILog')->create({
        parent_id            => $request->header('Request-APILog-Id'),
        Customer_id          => $request->authentication->{customer_id},
        APIProviderMethod_id => $APIProviderMethod->id,
        address              => $request->authentication->{source_ip_address} || $request->authentication->{ip_address},
        start                => time,
        request              => { $request->args('deref', 'insecure') },
        ($request->header('Admin-Superuser') ? (
        admin_su             => $request->header('Admin-Superuser'),
        ) : ()),
    });
    $request->heap_set('APILog' => $APILog);

    return;
}

sub precall_construct_objects { }

sub precall_validate { }

sub completed_request {
    my ($self, $request, $text_result) = @_;
    
    my $APILog = $request->heap_index('APILog');
    return if ! $APILog;
    
    my $heap_key = $text_result eq 'success' ? 'result' : $text_result;
    my $result = $request->heap_index($heap_key);
    if (ref $result && blessed $result && $result->isa('Thrift::Parser::Type')) {
        $result = $result->value_plain;
    }

    $APILog->update({
        duration => $request->heap_index('time_send_response') - $request->heap_index('time_off_wire'),
        result   => $result,
        is_error => $text_result eq 'success' ? 0 : 1,
    });

    return;
}

sub method_healthCheck {
    my ($self, $request) = @_;

    my %result;

    my $uptime = `uptime`;
    chomp $uptime;

    $result{start} = $self->{create_time};
    $result{hostname} = $self->{hostname};
    ($result{loadAverage}) = $uptime =~ m{load average: (.+)\s*$};

    $request->set_result(\%result);
}

sub method_ping {
    my ($self, $request) = @_;

    my %args = $request->args('deref');
    my %result;
    
    if (exists($args{echo}) && $args{echo} ne '') {
        $result{echo} = $args{echo};
    } else {
        $result{echo} = "No echo provided in call to method ping.";
    }

    $result{start} = $self->{create_time};
    $result{pong} = 'Pong!';

    $request->set_result(\%result);
}

1;
