package MyAPI::Server::ThriftAMQP;

=head1 NAME

MyAPI::Server::ThriftAMQP - An API server of Thrift over AMQP

=head1 SYNOPSIS

  use MyAPI::Server::ThriftAMQP;

  my $server = MyAPI::Server::ThriftAMQP->create(
      ThriftIDL => 'tutorial.thrift',
      Service   => 'Calculator',
      Handlers  => [{
          object => My::Calculator->new(),
      }],
  );

  $server->run;

=head1 DESCRIPTION

A subclass of L<MyAPI::Server> and L<MyAPI::Common::ThriftAMQP>.  Implements a L<Thrift::IDL> aware AMQP RPC service.

The process for handling requests is as follows:

=over 4

=item Generate a L<Thrift::IDL> object from the file passed.

=item Audit the thrift file using the style guide.

=item Create a L<Thrift::Parser> object and find the named service.

=item Connect to AMQP server and setup QOS so that we only receive one message at a time from any Consume call.

=item Create (if not present) a non-exclusive AMQP queue with the same name as the 'Service'

=item Consume this queue

=item If forking, create a L<POE::Component::IndependentProcesses> process manager, for which each child will open up their own separate AMQP connection.

=item As messages come in off the wire, do the following:

=over 4

=item Parse the payload using L<Thrift::Parser/parse_message>.  Send error response if failure.

=item Perform L<MyAPI::Server/is_valid_request> authentication check.

=item If forking, check for current queue capacity.  We don't want to hold onto more requests then we can handle immediately.  If we will be at max capacity after this message, push the Ack call onto a queue to be sent afer the next message is handled by the process manager.  Otherwise, send the ack call now so we can receive more messages.  Then, hand off the call to the process manager, which will use it's IPC to send the request to an idle child process.

=item If not forking, handle the request synchronously, sending the Ack call after the request is complete.

=back

=back

Handling requests is done by creating a L<MyAPI::MethodCall> object and handing it off to each L<MyAPI::Server::ThriftAMQP::Handler> object in my handler chain for actions to be added to the sequence.

When a MethodCall is finished, a response will be published to the queue indicated in the request's 'reply to' header frame field, of some type of Thrift message.

=cut

use strict;
use warnings;
use Carp;
use Data::Dumper;
use Scalar::Util qw(blessed);
use Params::Validate qw(validate ARRAYREF);
use Time::HiRes qw(gettimeofday tv_interval);
use Net::IP; # exports $IP_NO_OVERLAP
use base qw(
    MyAPI::Server
    MyAPI::Common::ThriftAMQP
);
__PACKAGE__->mk_group_accessors(simple => qw(
    amq
    debug
    logger
    channel
    queue
    parser
    service
    process_manager
));

use MyAPI::MethodCall;
use MyAPI::Server::ThriftAMQP::Handler;
use POE qw(
    Component::IndependentProcesses
);
use POE::Component::Client::AMQP qw(:constants);

# AMQP libraries
use Net::AMQP::Protocol::v0_8;
# AMQP debug
use YAML::XS qw();
use Term::ANSIColor qw(:constants);

# Thrift libraries
use Thrift::IDL;
use Thrift::Parser;

=head1 CLASS METHODS

=head2 create

Call with the following named values:

=over 4

=item I<ThriftIDL> (filename)

=item I<Service> (name of L<Thrift::IDL::Service> to implement)

=item I<Handlers>

Array of hashes, where each hash is passed to L<MyAPI::Server::ThriftAMQP::Handler/factory> and added to the handler chain.

=item I<Debug> (used for L<Thrift::Parser> debug)

=item I<Logger> (something like L<Log::Log4perl>; defaults to the L<POE::Component::Client::AMQP> logger object)

=item I<Fork>

=item I<PreFork>

=item I<MaxForks>

=item I<MaxRequests>

If 'Fork' is provided, these arguments are passed to L<POE::Component::IndependentProcesses> to spawn children which will handle the incoming requests.

=item I<RemoteAddress>

The AMQP server address.

=item I<RemotePort>

=item I<Username>

=item I<Password>

=item I<VirtualHost>

=item I<SSL>

These provide arguments to the L<POE::Component::Client::AMQP> object creation.

=item I<QueueName>

The AMQP queue to subscribe to.  If not present, defaults to the name of the Service

=back 4

=cut

sub create {
    my $class = shift;

    my %opts = validate(@_, {
        ThriftIDL => 1,
        Service   => 1,
        #Methods   => { type => ARRAYREF },
        Handlers  => { type => ARRAYREF },
        Debug     => 0,
        Logger    => 0,

        # Fork manager
        Fork      => 0,
        PreFork   => { default => 1 },
        MaxForks  => { default => 4 },
        MaxRequests => { default => 100 },

        # Opts for AMQP
        RemoteAddress  => 0,
        RemotePort     => 0,
        Username       => 0,
        Password       => 0,
        VirtualHost    => 0,
        SSL            => { default => 1 },,
        QueueName      => 0,
        Consume        => { default => 1 },
    });

    $opts{QueueName} ||= $opts{Service};

    my %self = (
        debug   => $opts{Debug},
        #methods => { map { $_ => 1 } @{ $opts{Methods} } },
        logger  => $opts{Logger},
        
        # Necessary for Common function service_call()
        opts => \%opts,
        calls => {},
        last_call_id => 0,
    );
    my $self = bless \%self, $class;

    foreach my $handler_details (@{ $opts{Handlers} }) {
        my $handler = MyAPI::Server::ThriftAMQP::Handler
            ->factory(%$handler_details);
        if (! $handler) {
            croak "Didn't find handler that matched signature: "
                .join(', ', sort keys %$handler_details);
        }
        push @{ $self{handlers} }, $handler;
    }

    $self{idl}     = Thrift::IDL->parse_thrift_file($opts{ThriftIDL}, $ENV{DEBUG});
    if (my @audit = MyAPI::Common::ThriftAMQP->audit_idl_document($self{idl})) {
        print "The IDL '$opts{ThriftIDL}' failed the MyAPI audit:\n";
        print " * $_\n" foreach @audit;
        exit 1;
    }

    $self{parser}  = Thrift::Parser->new(idl => $self{idl}, service => $opts{Service});
    $self{service} = $self{idl}->service_named($opts{Service});

    # Create 'service_parsers' for every type of service.  Necessary for Provider <=> Provider calls,
    # implemented by Common service_call()
    foreach my $service (@{ $self{idl}->services }) {
        $self{service_parsers}{$service->name} =
            Thrift::Parser->new(idl => $self{idl}, service => $service->name);
    }

    $self->open_amqp_connection();

    $self{logger} ||= $self->amq->Logger;
    $self{logger}->info("Will attempt to connect to ".$self->amq->{RemoteAddress}.":".$self->amq->{RemotePort}."");

    if (! $opts{Consume} && $opts{Fork}) {
        die "Cannot use 'Fork' and !'Consume' at the same time";
    }

    if ($opts{Consume}) {
        $self{queue} = $self{channel}->queue($opts{QueueName}, {
            durable => 1,
            exclusive => 0,
        });

        $self{queue}->subscribe(
            sub {
                my ($payload, $meta) = @_;
                $self->handle_amq_message($payload, $meta);
            },
            {
                no_ack => 0,
            }
        );
    }

    if ($opts{Fork}) {
        $self{queue}->do_when_created(sub {
            return if $self{process_manager}; # reconnected perhaps?
            $self{process_manager} = POE::Component::IndependentProcesses->new(
                logger       => $self->logger,
                alias        => 'process_manager',
                pre_fork     => $opts{PreFork},
                max_forks    => $opts{MaxForks},
                max_queue    => 0, # don't allow it to queue more than it can handle
                max_requests_per_child => $opts{MaxRequests},
                callbacks    => {
                    parent_forked => sub {
                        my ($child_pid) = @_;

                        $self->logger->info("Forked to $child_pid");
                    },
                    setup_child => sub {
                        # Shutdown the copied AMQP connection non-gracefully
                        $self->{amq}{is_stopping} = 1;
                        $poe_kernel->call( $self->{amq}{Alias}, 'shutdown' );

                        # Reconnect to AMQP as a client
                        $self->open_amqp_connection();

                        # Update the rand() seed so that I don't create the same random numbers as other children
                        srand(time ^ $$ ^ unpack "%L*", `ps axww | gzip -f`);

                        # By default, the child will be in idle state after it's forked and the parent <=> child socket
                        # is setup.  This will trigger a queue process immediately.  I don't want this, as we need to wait
                        # until the AMQP connection is open and idle before we should do a queue process.  Therefore,
                        # let's set our default state to something non-idle
                        $self->process_manager->update_status('wait_amqp');
                        $self->{amq}->do_when_startup(sub {
                            $self->process_manager->update_status('idle');
                        });
                    },
                    child_status_change => sub {
                        my ($old_status, $status) = @_;
                        return unless $status eq 'idle' && $old_status eq 'processing';

                        return unless $self->{pending_acks};
                        my $delivery_tag = shift @{ $self->{pending_acks} };
                        return unless defined $delivery_tag;

                        #$self->logger->info("Acking pending ack ($delivery_tag)");
                        $self->{channel}->send_frames(
                            Net::AMQP::Protocol::Basic::Ack->new(
                                delivery_tag => $delivery_tag,
                            )
                        );
                    },
                },
                child_methods => {
                    call_self_object_method => sub {
                        my ($oneway, %details) = @_;

                        my $method = $details{method};
                        my $return = $self->$method(@{ $details{args} });

                        if (! $oneway) {
                            if ($return->isa('POE::Component::Sequence')) {
                                $return->add_finally_callback(sub {
                                    $self->process_manager->update_status('idle');
                                });
                            }
                            else {
                                $self->logger->error("Method call $method didn't return a PoCo::Sequence object, so I have no way to know when to notify the process manager parent that we're idle once again");
                            }
                        }
                    },
                },
            );
        });
    }
    
    return $self;
}

sub open_amqp_connection {
    my $self = shift;

    $self->{amq} = POE::Component::Client::AMQP->create(
        Alias => 'amqp_server_' . $$,
        AliasTCP => 'tcp_server_' . $$,
        ($self->{opts}{Logger} ? (
        Logger => $self->{opts}{Logger},
        ) : ()),
        ($self->{opts}{Debug} ? (
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
        Keepalive => 60 * 5,

        Reconnect => 1,
        Callbacks => {
            Disconnected => [sub {
                my $amq = shift;
                $amq->Logger->info("We have been disconnected");
            }],
            Reconnected => [sub {
                my $amq = shift;
                $amq->Logger->info("We have been reconnected");
            }],
        },

        # Passthrough the AMQP opts from my opt hash
        map { $_ => $self->{opts}{$_} }
        grep { defined $self->{opts}{$_} }
        qw(RemoteAddress RemotePort Username Password VirtualHost SSL)
    );

    $self->{channel} = $self->{amq}->channel();

    # Only allow for one message to come down the pipe at a time
    $self->{channel}->send_frames(
        Net::AMQP::Protocol::Basic::Qos->new( prefetch_count => 1 ),
    );
}

=head2 run ()

Shortcut to $poe_kernel->run()

=cut

sub run {
    my $self = shift;
    $poe_kernel->run();
}

=head1 OBJECT METHODS

=head2 Accessors

=over 4

=item I<amq>

=item I<debug>

=item I<channel>

=item I<queue>

=item I<parser>

=item I<service>

=item I<logger>

=back

=cut

=head2 handle_amq_message ($payload, $meta)

Pass in the raw opaque Thrift payload and the meta data object that's part of the signature to the L<POE::Component::Client::AMQP::Queue::subscribe> call.

=cut

sub handle_amq_message {
    my ($self, $payload, $meta) = @_;

    my $time_off_wire = scalar(gettimeofday);

    # Parse the payload

    my $message;
    eval {
        die "No content type" if ! $meta->{header_frame}->content_type;
        my $protocol = $self->get_thrift_protocol($meta->{header_frame}->content_type, \$payload);
        $message = $self->parser->parse_message($protocol);
    };
    if ($@) {
        $self->send_amq_response({ error => "Invalid message: $@" }, $meta);
        return AMQP_ACK;
    }
    
    # Ensure that I'm wanting to handle this message

#    if (! $self->{methods}{ $message->method->name } && ! $self->{methods}{'*'}) {
#        # I'm not supposed to handle this request; don't ack it and do nothing
#        return 0; # don't ack
#    }

    # Perform authorization

    my $authentication;
    eval { 
        $authentication = $self->is_valid_request(
            headers => {
                content_type => $meta->{header_frame}->content_type,
                reply_to     => $meta->{header_frame}->reply_to,
                %{ $meta->{header_frame}->headers },
            },
            payload_ref => \$payload,
        );
    };
    if ($@) {
        $authentication = { error => "Internal error: Provider '" . $self->{opts}{Service} . "' died inside is_valid_request()" };
        $self->logger->error("is_valid_request() died with: $@");
    }
    if (! $authentication->{success}) {
        $self->send_amq_response($authentication, $meta);
        return AMQP_ACK;
    }

    my @args = ($message, $meta, $authentication, $time_off_wire);

    my $ack_message_sub = sub {
        $self->{channel}->send_frames(
            Net::AMQP::Protocol::Basic::Ack->new(
                delivery_tag => $meta->{method_frame}->delivery_tag,
            )
        );
    };

    my $enqueue_success;

    if ($self->{opts}{Fork}) {
        my $capacity = $self->process_manager->parent->queue_capacity;
        $self->logger->debug("Before handling, we have a queue capacity of $capacity");

        $enqueue_success = $self->process_manager->call_child_method(
            method => 'call_self_object_method',
            args => [
                method => 'child_handle_message',
                args => \@args,
            ],
            oneway => 0,
        );

        if (! $enqueue_success) {
            $self->logger->error("Unable to enqueue request in process manager; handling locally instead");
        }
        else {
            $self->logger->info("Enqueued request in process manager");
            # If, after calling enqueue_message, I'll have reached my queue capacity,
            # ask the process manager to ack the message on the server AFTER the next
            # message is handled.  Otherwise, ack it now so I can get more messages.
            if (($capacity - 1) <= 0) {
                #$self->logger->info("Acking later");
                # This is handled by 'child_status_change' (above)
                push @{ $self->{pending_acks} }, $meta->{method_frame}->delivery_tag;
            }
            else {
                $ack_message_sub->();
            }
        }
    }

    if (! $enqueue_success) {
        my $sequence = $self->child_handle_message(@args);
        $sequence->add_finally_callback($ack_message_sub);
    }

    return;
}

sub child_handle_message {
    my ($self, $message, $meta, $authentication, $time_off_wire) = @_;

    # Perform the method call, deferred

    my $call = MyAPI::MethodCall->new(
        service   => $self->service,
        method    => $message->method->idl,
        arguments => $message->arguments,
        message   => $message,
        transport => $meta,
        logger    => $self->logger,
        authentication => $authentication,
        server    => $self,
    );

    $call->heap_set('time_off_wire' => $time_off_wire);

    $call->add_action(\&authorization);

    # Allow each user-defined handler to add actions to the call chain
    foreach my $handler (@{ $self->{handlers} }) {
        $handler->add_actions($call);
    }

    $call->add_error_callback(sub {
        my ($sequence, $error) = @_;
        $sequence->logger->error("Sequence generated an error: $error");
        if (defined $error && ref $error && blessed $error) {
            $sequence->heap_set(exception => $error);
        }
        else {
            $sequence->heap_set(error => $error);
        }
    });

    $call->add_finally_callback(sub {
        my $sequence = shift;
        $self->method_call_finally($sequence, $message, $meta);
        $self->method_call_finally_amq($sequence, $message, $meta);
    });

    $call->run();

    return $call;
}

sub local_handle_message {
    my ($self, $message) = @_;
    my $meta = undef;

    my $call = MyAPI::MethodCall->new(
        service   => $self->service,
        method    => $message->method->idl,
        arguments => $message->arguments,
        message   => $message,
        transport => $meta,
        logger    => $self->logger,
        authentication => {},
        server    => $self,
    );

    # Allow each user-defined handler to add actions to the call chain
    foreach my $handler (@{ $self->{handlers} }) {
        $handler->add_actions($call);
    }

    $call->add_error_callback(sub {
        my ($sequence, $error) = @_;
        if (defined $error && ref $error && blessed $error) {
            $sequence->heap_set(exception => $error);
        }
        else {
            $sequence->heap_set(error => $error);
        }
    });

    $call->add_finally_callback(sub {
        my $sequence = shift;
        $self->method_call_finally($sequence, $message, $meta);
    });

    $call->run();

    return $call;
}

sub method_call_finally {
    my ($self, $method_call, $request, $meta) = @_;
            
    my $response;
    if ($method_call->heap_exists('error')) {
        my $error = $method_call->heap_index('error');

        # Ensure error is plain text and not an exception
        if (ref $error) {
            $method_call->heap_reset('error');
            $method_call->heap_set(exception => $error);
            return $self->method_call_finally($method_call, $request, $meta);
        }

        $response = $request->compose_reply_application_exception(
            $error, TApplicationException::UNKNOWN
        );
    }
    elsif ($method_call->heap_exists('exception')) {
        my $exception = $method_call->heap_index('exception');

        # Ensure exception is actually an exception and not just a plain error
        if (! defined $exception || ! ref $exception) {
            $method_call->heap_set(error => $exception);
            return $self->method_call_finally($method_call, $request, $meta);
        }

        # Check the type of the exception; if it's a user-defined exception and is
        # one of the throwable types, compose the message as a REPLY.  Otherwise,
        # compose as a generic EXCEPTION.
        if (! blessed($exception) && ref($exception) eq 'HASH') {
            my @keys = keys %$exception;
            if (int @keys == 1 && $request->method->throw_classes->{$keys[0]}) {
                $response = $request->compose_reply_exception($exception);
            }
        }
        elsif (blessed($exception) && $exception->isa('Thrift::Parser::Type::Exception')) {
            my %throw_classes = reverse %{ $request->method->throw_classes };
            if ($throw_classes{ ref($exception) }) {
                $response = $request->compose_reply_exception($exception);
            }
        }
        elsif (blessed($exception) && $exception->isa('TApplicationException')) {
            $response = $request->compose_reply_application_exception(
                $exception->getMessage, $exception->getCode,
            );
        }

        # If not matched exception, treat as plain text error
        if (! defined $response) {
            $method_call->heap_set(error => $exception . '');
            return $self->method_call_finally($method_call, $request, $meta);
        }
    }
    elsif ($method_call->heap_exists('result')) {
        # Wrap compose_reply() in an eval, as it may throw an exception if the
        # method-returned value doesn't validate.  Re-call function if the case.
        $response = eval { $request->compose_reply($method_call->heap_index('result')) };
        if ($@) {
            $method_call->heap_set(error => $@ . '');
            return $self->method_call_finally($method_call, $request, $meta);
        }
    }

    # If we couldn't find a result, throw a relevant application exception
    if (! $response) {
        $method_call->heap_set(error => "Missing result");
        $response = $request->compose_reply_application_exception(
            'Missing result', TApplicationException::MISSING_RESULT
        );
    }

    $method_call->heap_set(result_thrift => $response);
}

sub method_call_finally_amq {
    my ($self, $method_call, $request, $meta) = @_;
    my $response = $method_call->heap_index('result_thrift');

    my $protocol = $self->get_thrift_protocol($meta->{header_frame}->content_type);
    $response->write($protocol);

    $self->send_amq_response(
        { success => $protocol->getTransport->getBuffer },
        $meta, $method_call,
    );
}

sub send_amq_response {
    my ($self, $result, $meta, $method_call) = @_;

    # Passthrough the request headers to the response save for those that were
    # are reserved for this app.  This allows the requestor to insert context
    # for the call that they can see when they get the result.
    my %response_headers = $meta->{header_frame}->headers ? %{ $meta->{header_frame}->headers } : ();
    delete $response_headers{$_} foreach @MyAPI::Server::reserved_headers;

    my %header_frame = (
        content_type => 'application/x-thrift',
        delivery_mode => 1,
        priority => 1,
        headers => {
            %response_headers,
            'Status-Code' => 200,
        },
    );

    if ($result->{error} && ! $meta->{header_frame}->reply_to) {
        $self->logger->error("Service request had an error '$$result{error}' but no reply_to address so requestor will not be notified");
        return;
    }

    ## Construct a response

    my $response = '';
    if (defined $result->{success}) {
        $response = $result->{success};
    }
    else {
        $result->{code} ||= 400;
        $self->logger->error("Service request had an error '$$result{code} $$result{error}'");
        $header_frame{headers}{'Status-Code'} = $result->{code};
        $header_frame{content_type} = 'text/plain';
        $response = $result->{error};
    }

    ## Send the response to the 'reply_to' queue

    $self->{channel}->send_frames(
        $self->amq->compose_basic_publish(
            $response, 

            # Net::AMQP::Protocol::Basic::Publish
            ticket => 0,
            routing_key => $meta->{header_frame}->reply_to,
            mandatory => 1,

            # Net::AMQP::Frame::Header
            weight       => 0,
            %header_frame,
        )
    );

    if ($method_call) {
        $method_call->heap_set('time_send_response' => scalar(gettimeofday));

        # Print timing information

        my %timing;
        foreach my $key (sort $method_call->heap_keys) {
            my ($time_key) = $key =~ m{^time_(.+)$};
            next unless $time_key;
            $timing{$time_key} = $method_call->heap_index($key);
        }

		my $method_result = $method_call->heap_exists('error') ? 'error' :
			$method_call->heap_exists('exception') ? 'exception' : 'success';

        # Allow each user-defined handler to know the result
        foreach my $handler (@{ $self->{handlers} }) {
            $handler->request_complete($method_call, $method_result);
        }

        if ($ENV{DEBUG}) {
            $self->logger->debug(sprintf "Completed %s.%s with result %s",
                $method_call->service->name,
                $method_call->method->name,
                $method_result
            );
            $self->logger->debug(Dumper({
                request => { $method_call->args('deref') },
                # Dereference exceptions and blessed errors
                map { $_ => $_ eq 'result' ? $method_call->heap_index($_) : $method_call->heap_index($_) . '' }
                grep { $method_call->heap_exists($_) }
                qw(error exception result)
            }));
            $self->logger->debug(sprintf "%20s: %f", $_ => $timing{$_})
                foreach sort { $timing{$a} <=> $timing{$b} } keys %timing;
        }
    }
}

=head2 is_valid_request

Supplements the parent 'is_valid_request' method; checks for valid 'application/x-thrift' content type and the presence of the 'reply to' header frame field.

=cut

sub is_valid_request {
    my ($self, %opt) = @_;

    ## Ensure proper form of the AMQP payload

    my $content_type = $opt{headers}{content_type};
    if ($content_type !~ m{^application/x-thrift}) {
        return { error => "Invalid content type '$content_type'; no way to handle that" };
    }

    my $reply_to = $opt{headers}{reply_to};
    if (! $reply_to) {
        return { error => "Received request but no indicated reply_to queue" };
    }

	return { success => 1 };
}

=head2 authorization

Uses the L<authentication> to perform authorization on the method call and upon each parameter passed.  Sets an error or exception if it fails the authorization, proceeding no further with the actions.

=cut

sub authorization {
    my $call = shift;

    $call->heap_set(time_authorization => scalar(gettimeofday));

    if (! $call->authentication) {
        die "Request was not authorized\n";
    }

	#my $roles = $call->authentication->{roles};
	#if (! $roles) {
	#    die "No roles present in MethodCall authentication\n";
	#}

    # Perform method and parameter validation
    eval {
        MyAPI::Common::ThriftAMQP
            ->validate_parser_message($call->message, {
					#roles    => [ keys %$roles ],
					#key_type => $call->authentication->{key_type},
            });
    };
    if (my $e = MyAPI::InvalidArgument->caught()) {
		# FIXME: Return a common invalid argument result
		#return $call->set_exception(
		#    MyClass::InvalidArguments->compose({
		#        argument => $e->key,
		#        message  => $e->error,
		#    })
		#);
    }
    elsif ($@) {
        return $call->set_error($@);
    }

    return;
}

1;
