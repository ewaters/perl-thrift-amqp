package MyAPI::JSONProxy;

=head1 NAME

MyAPI::JSONProxy - RESTful JSON proxy for the Thrift AMQP API

=head1 DESCRIPTION

Run a L<POE::Component::Server::HTTP> that receives requests such as 'POST /Container/reboot HTTP/1.0' and, given headers that authorize the request, encode a Thrift payload and enqueue the request on the AMQP servers, returning a response to the request when an AMQP response is returned.

MyAPI::JSONProxy is useful for customers to use directly, providing them with an easy to use interface to our API that doesn't entail complicated AMQP coding.  They also don't require any Thrift code, as the payload is encoded as JSON.

This is also useful internally to provide us with a synchronous interface to our API for tasks that need to block on a particular method call.

The biggest issue with using this proxy for making API calls is the amount of time a call may need to complete.  AMQP has no timeouts, and a request may take a very long time to complete.  HTTP, however, has timeouts, so this proxy is best used for shorter requests.

=cut

use strict;
use warnings;

# This is a custom modified version of POE::Component::Server::HTTP that supports SSL
use POE qw(Component::Server::HTTPs);
use MyAPI::Client::ThriftAMQP;
use MyAPI::Server;
use HTTP::Status qw(:constants);
use JSON::XS;
use Data::UUID;
use Data::Dumper;
use Scalar::Util qw(blessed);
use Params::Validate qw(validate validate_with);
use Time::HiRes qw();

use Log::Log4perl qw(get_logger :levels);
use Log::Log4perl::Appender;
use Log::Log4perl::Layout;
use Log::Dispatch::Syslog;

## Configuration

my %opt = (
    http_server_port   => 3016,
    alias              => 'apiproxy',
    thrift_idl         => undef,
    ssl_key            => undef,
    ssl_cert           => undef,
    amq_virtual_host   => undef,
    amq_remote_address => undef,
	amq_remote_port    => undef,
    amq_ssl            => undef,

    # To allow MyAPI::JSONProxy to inject the 'JSONProxy-Remote-IP' header, at the API::Provider end
    # we need a way to trust that this is genuine.  This is that assurance.
    SharedSecret => 'zeshcabyie',

    log_debug  => 1,
    log_syslog => 0,
    log_screen => 1,
    log_file   => undef,
    log_facility => 'local4',
);

## Setup

my $data_uuid = Data::UUID->new();
my $logger;
my $json_xs = JSON::XS->new->utf8->allow_nonref;

=head1 METHODS

=head2 log

Returns a logger

=cut

sub log {
	my $class = shift;
	$logger ||= $class->create_logger();
	return $logger;
}

=head2 config

Returns a hashref of the configuration

=cut

sub config {
    return \%opt;
}

=head2 run (\%config)

$config is a hashref of lots of different things; only grab those that are relevant.

=cut

sub run {
    my ($class, $config) = @_;

    foreach my $key (grep { exists $opt{$_} } keys %$config) {
        $opt{$key} = $config->{$key};
    }

    validate_with(
        params => \%opt,
        spec => {
            thrift_idl => { callbacks => { 'is_file' => sub { -f shift } } },
            ssl_key    => { callbacks => { 'is_file' => sub { -f shift } } },
            ssl_cert   => { callbacks => { 'is_file' => sub { -f shift } } },
        },
        allow_extra => 1,
    );

    # Create logger (if not already created) by calling the accessor
    $class->log; #->debug(Dumper(\%opt));

    # Create controlling session
    POE::Session->create(
        inline_states => {
            _start => \&start,

            http_server_default_handler => \&http_server_default_handler,
            proxy_request  => \&proxy_request,

            start_request  => \&start_request,
            finish_request => \&finish_request,

            health         => \&health,
        },
        heap => {
            %opt,
            logger => $logger,
        },
    );

    $poe_kernel->run();
}

=head1 POE STATES

=head2 start

Setup an HTTPS server and a Thrift AMQP client.

=cut

sub start {
    my ($kernel, $heap) = @_[KERNEL, HEAP];

    $kernel->alias_set($heap->{alias});

    $heap->{http_aliases} = POE::Component::Server::HTTPs->new(
        Port => $heap->{http_server_port},
        ContentHandler => {
            '/' => sub {
                $kernel->call($heap->{alias}, 'http_server_default_handler', @_);
            },
            '/health' => sub {
                $kernel->call($heap->{alias}, 'health', @_);
            },
        },
        SSL => {
            KeyFile => $heap->{ssl_key},
            CertFile => $heap->{ssl_cert},
        },
    );

    $heap->{myapi_client} = MyAPI::Client::ThriftAMQP->new(
        Logger      => $heap->{logger},
        Keepalive   => 60 * 5,
        Debug       => ($ENV{DEBUG} ? 1 : 0),
        ThriftIDL   => $heap->{thrift_idl},
        VirtualHost => $heap->{amq_virtual_host},
        ($heap->{amq_remote_address} ? (
        RemoteAddress => $heap->{amq_remote_address},
        ) : ()),
        ($heap->{amq_remote_port} ? (
        RemotePort => $heap->{amq_remote_port},
        ) : ()),
        (defined $heap->{amq_ssl} ? (
        SSL => $heap->{amq_ssl},
        ) : ()),
        Reconnect => 1,
    );
	
	# Figure out what Perl namespace was used in the thrift IDL
	$heap->{message_namespace} = $heap->{myapi_client}{idl}->headers->[0]->namespace('perl');

    $heap->{logger}->info("Connect to the API proxy on the HTTPS port $opt{http_server_port}");
}

=head2 http_server_default_handler

Decode and authorize API requests, preparing them for C<start_request>

=cut

sub http_server_default_handler {
    my ($kernel, $heap, $request, $response) = @_[KERNEL, HEAP, ARG0, ARG1];

    my $ip = $request->header('X-Forwarded-For') || $request->{connection}{remote_ip};
    $heap->{logger}->info("Request for ".uc($request->method)." '".$request->uri->path."' from $ip");
    if ($ENV{DEBUG}) {
        $heap->{logger}->debug($request->as_string);
    }

    ## Decode the content

    my ($data, %headers);
    eval {
		# Was content sent?
		if ($request->header('Content-Type')) {
			if ($request->header('Content-Type') !~ m{^application/json\b}) {
				die "Invalid 'Content-Type' header; must be 'application/json'\n";
			}

			# Decode the payload
			my $payload = $request->content;
			eval { $data = $json_xs->decode($payload) };
			if ($@) {
				die "Failed to decode JSON data in request payload: $@\n";
			}
		}
		# Perhaps encoded on the URI
		else {
			my %query_parameters = $request->uri->query_form();
			$data = \%query_parameters;
		}
    };
    if (my $ex = $@) {
        $heap->{logger}->error($ex);
        $response->code(HTTP_BAD_REQUEST);
        $response->content_type('application/json; charset=utf8');
        $response->content($json_xs->encode({ message => $ex, success => 0 }));
        return RC_OK;
    }

    if ($data->{_myapi_request_id}) {
        $headers{RequestID} = delete $data->{_myapi_request_id};
    }

    ## Dispatch accordingly

    my $id = $data_uuid->create_str;
    
    my $api_request = {
        start    => Time::HiRes::time,
        request  => $request,
        response => $response,
        ip       => $ip,
        data     => $data,
        header_data => \%headers,
        id       => $id,
    };
    $api_request->{times}{start} = $api_request->{start};
    $response->streaming(1);

    $heap->{requests_by_ip}{$ip}{$id}++;
    $heap->{requests_by_path}{ $request->uri->path }{$id}++;
    $heap->{requests}{$id} = $api_request;

	$kernel->yield('start_request', $api_request);

    return RC_WAIT;
}

=head2 start_request

Compose a Thrift payload, setup an action chain, and call MyAPI::Client::ThriftAMQP->service_call() to handle the request.

=cut

sub start_request {
    my ($kernel, $heap, $details) = @_[KERNEL, HEAP, ARG0];

    $details->{times}{start_request} = Time::HiRes::time;

    my $client = $heap->{myapi_client};
    my ($queue_name, $method) = $details->{request}->uri->path =~ m{^/([^/]+)/([^/]+)$};
    if (! $queue_name) {
        $details->{error} = "Invalid request.  Only understand '/health' or '/<service>/<method>'";
        $kernel->post($heap->{alias}, 'finish_request', $details);
        return;
    }

    # The service name is the same as the queue name, sans an optional '.<instance_id>'
    # i.e., 'DatabaseInstance.14' is the 14th instance of the DatabaseInstance service
    my $service = $queue_name;
    $service =~ s{\..+}{};

    my $method_class = join '::', $heap->{message_namespace}, $service, $method;
    $details->{method_class} = $method_class;

    if ($ENV{DEBUG}) {
        $heap->{logger}->debug("Generating thrift message for '$method_class' with: " . Dumper($details->{data}));
    }
        
    my $message = eval { $method_class->compose_message_call(
        %{ $details->{data} }
    ) };
    if (my $ex = $@) {
        if ($ex =~ m{Can't locate object method "compose_message_call"}) {
            $ex = "invalid service method '$service.$method'";
        }
        $details->{error} = "Failed to construct API request: $ex";
        $kernel->post($heap->{alias}, 'finish_request', $details);
        return;
    }

    $details->{times}{after_compose_message} = Time::HiRes::time;

    # Setup response callbacks
    my $sequence = POE::Component::Sequence->new()
        ->add_action(sub {
            $details->{times}{before_thrift_parse} = Time::HiRes::time;
        })
        ->add_callback(sub {
            my ($sequence, $return) = @_;
            $details->{return} = $return->value_plain;
            $details->{times}{after_thrift_dereferenced} = Time::HiRes::time;
        })
        ->add_error_callback(sub {
            my ($sequence, $error) = @_;
            $client->logger->error("An error occurred on ".$message->method.", error:".Dumper($error));

            if (ref $error && blessed $error) {
                if ($error->isa('MyAPI::InvalidArguments')) {
                    $details->{error} = "The argument '".$error->named('argument')."' had an error: ".$error->named('message');
                }
                elsif ($error->isa('TApplicationException')) {
                    $details->{error} = "Application exception: code = ".$error->getCode.", message = ".($error->getMessage || 'undef');
                }
                else {
                    $details->{error} = "Totally unexpected error: ".Dumper($error);
                }
            }
            elsif ($error =~ m{NOT_FOUND - no queue '(.+?)' in vhost '(.+?)'}) {
                $details->{error} = "The service '$1' was not found; it may be unavailable or non-existant";
            }
            else {
                $details->{error} = "Protocol/transport error: $error";
            }
        })
        ->add_finally_callback(sub {
            $kernel->post($heap->{alias}, 'finish_request', $details);
        });

    # Pass the header_data as %call_opts
    my %call_opts = (
        %{ $details->{header_data} },
        headers => {
            'JSONProxy-Remote-IP' => $details->{ip},
            'JSONProxy-Shared-Secret' => $opt{SharedSecret},
            ($details->{'Request-APILog-Id'} ? (
            'Request-APILog-Id' => $details->{'Request-APILog-Id'},
            ) : ()),
        },
        queue_name => $queue_name,
    );

    eval {
        $client->service_call($message, $sequence, \%call_opts);
    };
    if ($@) {
        $details->{error} = "Failed to enqueue API request: $@";
        $kernel->post($heap->{alias}, 'finish_request', $details);
        return;
    }

    $details->{times}{after_service_call} = Time::HiRes::time;
}

=head2 finish_request

Encode a response, either a success or failure, and send it back to the requestor, completing the HTTP request.

=cut

sub finish_request {
    my ($kernel, $heap, $details) = @_[KERNEL, HEAP, ARG0];

    $details->{times}{finish_request} = Time::HiRes::time;

    my %response;
    if (exists $details->{return}) {
        $response{success} = 1;
        $response{result} = $details->{return};
    }
    else {
        $response{success} = 0;
        $response{error} = $details->{error} || "Unknown error";
    }

    if ($ENV{DEBUG}) {
        $heap->{logger}->debug("Completing '$$details{method_class}' request with response: " . Dumper(\%response));
    }

    # Compose the HTTP::Response
    $details->{response}->header('Content-Type' => 'application/json; charset=utf8');
    $details->{response}->code(RC_OK);
    $details->{response}->content($json_xs->encode(\%response));

    $details->{times}{after_encode_json} = Time::HiRes::time;

    # Close the POE::Component::Client::HTTP::Request and HTTP::Response objects
    # This will trigger the socket to be written to and closed.
    if ($details->{response}->streaming) {
        $details->{response}->send( $details->{response} );
        $details->{response}->close();
        $details->{request}->header(Connection => 'close');
    }

    # Clear heap references to this request
    my $id = $details->{id};
    delete $heap->{requests}{$id};
    delete $heap->{requests_by_ip}{ $details->{ip} }{$id};
    delete $heap->{requests_by_path}{ $details->{request}->uri->path }{$id};

    my @sorted_time_names = sort { $details->{times}{$a} <=> $details->{times}{$b} } keys %{ $details->{times} };
    for (my $i = 0; $i <= $#sorted_time_names; $i++) {
        my $time_name = $sorted_time_names[$i];
        my $diff = $i > 0 ? $details->{times}{$time_name} - $details->{times}{ $sorted_time_names[$i - 1] } : 0;
        $heap->{logger}->debug(sprintf "Time %s: %.3f (%.2f)", $time_name, $details->{times}{$time_name} - $details->{times}{start}, $diff);
    }
}

=head2 health

Return an HTML document with a table of all the currently pending requests.

=cut

sub health {
    my ($kernel, $heap, $request, $response) = @_[KERNEL, HEAP, ARG0, ARG1];

    my $content = <<EOF;
<html>

<head>
    <title>JSONProxy Health</title>
    <style>

    tr th {
        border-bottom: 1px solid black;
    }

    table {
        border: 1px solid black;
    }
    </style>

</head>
<body>
EOF

    my $draw_table = sub {
        my %opts = @_;

        $content .= "<h1>$opts{title}</h1>\n";
        $content .= "<table><tr>" . join('', map { "<th>$_</th>" } @{ $opts{columns} }) . "</tr>";
        foreach my $row (@{ $opts{rows} }) {
            $content .= "<tr>" . join('', map { "<td>$_</td>" } @$row) . "</tr>";
        }
        if (! int @{ $opts{rows} }) {
            $content .= "<tr><td colspan=".int(@{ $opts{columns} }).">No data</td></tr>";
        }
        $content .= "</table>";
    };

    $draw_table->(
        title => 'Requests by IP',
        columns => [ 'IP', 'Count' ],
        rows => [
            grep { $_->[1] > 0 }
            map {
                [ $_, int keys %{ $heap->{requests_by_ip}{$_} } ]
            }
            sort keys %{ $heap->{requests_by_ip} }
        ],
    );

    $draw_table->(
        title => 'Requests by Path',
        columns => [ 'Path', 'Count' ],
        rows => [
            grep { $_->[1] > 0 }
            map {
                [ $_, int keys %{ $heap->{requests_by_path}{$_} } ]
            }
            sort keys %{ $heap->{requests_by_path} }
        ],
    );

    my $now = Time::HiRes::time;
    $draw_table->(
        title => 'Requests by Age',
        columns => [ 'Age', 'Details' ],
        rows => [
            map {
                my $details = $heap->{requests}{$_};
                my $age = sprintf '%.2f', $now - $details->{start};
                [
                    "$age sec", 
                    "Path: ".$details->{request}->uri->path . "; IP: ".$details->{ip}
                ]
            }
            sort { $heap->{requests}{$a}{start} <=> $heap->{requests}{$b}{start} } keys %{ $heap->{requests} }
        ],
    );

    $response->content_type('text/html');
    $response->code(RC_OK);
    $response->content($content);
    return RC_OK;
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

	return $logger;
}

=head1 AUTHOR

Eric Waters

=cut

1;
