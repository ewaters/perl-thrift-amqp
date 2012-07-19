package MyAPI::Common::ThriftAMQP;

use strict;
use warnings;
use base qw(Class::Accessor::Grouped);

use Thrift::MemoryBuffer;
use Thrift::JSONProtocol;
use Thrift::BinaryProtocol;
use MyAPI::Exceptions;
use Data::Dumper;
use JSON::XS;

my $jsonxs = JSON::XS->new->ascii->allow_nonref;

my %content_type2thrift_protocol = (
    'application/x-thrift-json' => 'JSON',
    'application/x-thrift'      => 'Binary',
);

our %audit_style = (
    audit_types => 1,
    warn_unused_types => 0,
    docs => {
        require_return => 0,      # @return not required
        require_all_params => 0,  # @param's must match with method arguments
        require_all_methods => 1, # every method must be documented

        require_all_typedefs => 0,
        require_all_exceptions => 1,
        require_all_structs => 1,

        require_roles => 1,
    },
);

# List the '@' keys that are flags and will have no value following them
our %audit_documentation_flags = (
    optional => 1,
    utf8     => 1,
    secure   => 1,
);

=head2 get_thrift_protocol ($content_type, \$payload)

The L<Thrift::Parser> requires a L<Thrift::Protocol> object for it's (de)serialization routines.  Pass in a content type and get back a ready-to-use Protocol object with optionally your payload written to its' buffer.

=cut

sub get_thrift_protocol {
    my ($self, $content_type, $payload_ref) = @_;

    my $type = $content_type2thrift_protocol{$content_type}
        or die "Invalid content type '$content_type'; no valid Thrift Protocol library to handle it";

    if (! $self->{protocols}{$type}) {
        my $protocol_class = 'Thrift::' . $type . 'Protocol';
        my $buffer = Thrift::MemoryBuffer->new();
        $buffer->write($$payload_ref) if $payload_ref;
        my $protocol = $protocol_class->new($buffer);
        $self->{protocols}{$type} = $protocol;
        return $protocol;
    }

    my $protocol = $self->{protocols}{$type};
    my $buffer = $protocol->getTransport;
    $buffer->resetBuffer();
    $buffer->write($$payload_ref) if $payload_ref;
    return $protocol;
}

=head2 audit_idl_document ($thrift_idl_document)

=cut

sub audit_idl_document {
    my ($class, $document, $custom_types) = @_;

    $custom_types ||= {};

    my (%methods_types_used);

    my @audit;
    foreach my $child (@{ $document->children }) {
        foreach my $type (_audit_flatten_types($child)) {
            next unless $type->isa('Thrift::IDL::Type::Custom');
            if ($child->isa('Thrift::IDL::Service')) {
                $methods_types_used{ $type->full_name }++;
            }
            if ($audit_style{audit_types} && ! $custom_types->{ $type->full_name }) {
                push @audit, "$child contains reference to custom type ".$type->full_name." which hasn't yet been defined";
            }
        }

        if ($child->isa('Thrift::IDL::TypeDef')
            || $child->isa('Thrift::IDL::Struct')
            || $child->isa('Thrift::IDL::Enum')
            || $child->isa('Thrift::IDL::Senum')
        ) {
            _audit_parse_structured_comment($child);
            $custom_types->{ $child->full_name } = $child;
        }

        if ($audit_style{docs}{require_all_exceptions} && $child->isa('Thrift::IDL::Exception')) {
            my $failed = _audit_parse_structured_comment($child);
            push @audit, $failed if $failed;
        }
        elsif ($audit_style{docs}{require_all_structs} && $child->isa('Thrift::IDL::Struct') && ! $child->isa('Thrift::IDL::Exception')) {
            my $failed = _audit_parse_structured_comment($child);
            push @audit, $failed if $failed;
        }
        elsif ($child->isa('Thrift::IDL::Service')) {
            my $service = $child;
            _audit_parse_structured_comment($service);

            # Ensure that each child method has a comment that defines it's documentation,
            # and parse this comment into a data structure and store in the $method object.
            foreach my $method (@{ $child->methods }) {
                my %ids;
                foreach my $field (@{ $method->arguments }) {
                    # Check for 'optional' flag
                    if ($field->optional) {
                        push @audit, "Service $child method $method field '".$field->name."' has 'optional' flag: not valid for an argument field; rewrite as '\@optional' in comment";
                    }

                    # Check for non-duplication of field ids
                    if (! defined $ids{$field->id}) {
                        $ids{$field->id} = $field;;
                    }
                    else {
                        push @audit, "Service $child method $method field '".$field->name."' id ".$field->id." was already assigned to field '".$ids{$field->id}->name."'";
                    }
                }

                if (! $method->comments) {
                    if ($audit_style{docs}{require_all_methods}) {
                        push @audit, "Service $child method $method has no comment documentation";
                    }
                    next;
                }

                my $failed = _audit_parse_structured_comment($method);
                if ($failed) {
                    push @audit, $failed;
                    next;
                }

                # Check the structured documentation
                $method->{doc}{role} ||= $service->{doc}{role};
                if ($audit_style{docs}{require_roles} && ! $method->{doc}{role}) {
                    push @audit, "Service $child method $method documentation doesn't include a role";
                }
                $method->{doc}{permitKey} ||= $service->{doc}{permitKey};
            }
        }
    }

    if ($audit_style{warn_unused_types} && (my @unused = sort grep { ! $methods_types_used{$_} } keys %$custom_types)) {
        print "AUDIT WARN: The following types were custom defined but weren't used in any method I saw:\n";
        print "  " . join(', ', @unused) . "\n";
    }

    return @audit;
}

sub _audit_flatten_types {
    my ($node, $custom_types) = @_;
    if (! defined $node) {
        print STDERR "_audit_flatten_types() called on undef\n";
        return ();
    }
    # Resolve named custom types to their original typedef objects
    if ($node->isa('Thrift::IDL::Type::Custom') && $custom_types && $custom_types->{ $node->full_name }) {
        $node = $custom_types->{ $node->full_name };
    }
    my @types = map { $node->$_ } grep { $node->can($_) } qw(type val_type key_type returns);
    my @children = map { ref $_ ? @$_ : $_ } map { $node->$_ } grep { $node->can($_) && defined $node->$_ } qw(arguments throws children fields);
    return @types, map { _audit_flatten_types($_, $custom_types) } @types, @children;
}

sub _audit_parse_structured_comment {
    my ($object) = @_;

    my @comments = @{ $object->comments };

    if (! @comments) {
        return "$object has no comments";
    }

    my $comment = join "\n", map { $_->escaped_value } @comments;

    my %doc = ( param => {} );
    foreach my $line (split /\n\s*/, $comment) {
        my @parts = split /\s* (\@\w+) \s*/x, $line;
        while (defined (my $part = shift @parts)) {
            next if ! length $part;
            if (my ($javadoc_key) = $part =~ m{^\@(\w+)$}) {
                if ($audit_documentation_flags{$javadoc_key}) {
                    $doc{$javadoc_key} = 1;
                }
                else {
                    my $value = shift @parts;
                    if ($javadoc_key eq 'param') {
                        my ($param, $description) = $value =~ m{^(\w+) \s* (.*)$}x;
                        $doc{$javadoc_key}{$param} = $description || '';
                    }
                    elsif ($javadoc_key eq 'validate') {
                        my ($param, $args) = $value =~ m{^(\w+) \s* (.*)$}x;
                        push @{ $doc{$javadoc_key}{$param} }, $args || '';
                    }
                    else {
                        push @{ $doc{$javadoc_key} }, $value;
                    }
                }
            }
            else {
                if (! defined $doc{description}) {
                    $doc{description} = $part;
                }
                else {
                    $doc{description} .= "\n" . $part;
                }
            }
        }
    }


    # Allow fields/arguments to have structured comments and to describe themselves
    {
        # Only use arguments or fields, not both (some objects allow both)
        my @keys = grep { $object->can($_) } qw(arguments fields);
        if (@keys) {
            my $key = $keys[0];
            my @fields = @{ $object->$key };
            foreach my $field (@fields) {
                _audit_parse_structured_comment($field) if int @{ $field->comments };
                if ($field->{doc} && (my $description = $field->{doc}{description})) {
                    if (defined $doc{param}{$field->name}) {
                        $doc{param}{$field->name} .= '; ' . $description;
                    }
                    else {
                        $doc{param}{$field->name} = $description;
                    }
                }
            }
        }
    }

    # Check for completeness of the documentation

    if ($object->can('returns') && $audit_style{docs}{require_return}) {
        # Non-void return value requires description
        my $return = $object->returns;
        unless ($return->isa('Thrift::IDL::Type::Base') && $return->name eq 'void') {
            if (! $doc{return}) {
                return "$object has a non-void return value but no docs for it";
            }
        }
    }

    if ($audit_style{docs}{require_all_params}) {
        # Check the params (make a copy as I'll be destructive)
        my %params = %{ $doc{param} };
        my @fields = map { @{ $object->$_ } } grep { $object->can($_) } qw(arguments fields);
        foreach my $field (@fields) {
            if (defined $params{$field->name}) {
                delete $params{$field->name};
            }
            else {
                return "$object doesn't document argument $field";
            }
        }
        foreach my $remaining (keys %params) {
            return "$object documented param $remaining which doesn't exist in the object fields";
        }
    }

    $object->{doc} = \%doc;

    return;
}

sub validate_parser_message {
    my ($class, $message, $opt) = @_;
    $opt ||= {};

    my $idl = $message->method->idl_doc; 
    my $method_doc = $message->{method}->idl->{doc};
    my $bypass_roles = 0;

    # If the spec will permit by key type, bypass role check if the passed key type matches
    if ($opt->{key_type} && $method_doc->{permitKey}) {
        my %permit = map { $_ => 1 } @{ $method_doc->{permitKey} };
        if ($permit{$opt->{key_type}}) {
            $bypass_roles = 1;
        }
    }

    if (! $bypass_roles && $opt->{roles}) {
        my %doc_roles = map { $_ => 1 } @{ $method_doc->{role} };
        my @overlap = grep { $doc_roles{$_} } @{ $opt->{roles} };
        if (! @overlap) {
            MyAPI::Unauthorized->throw(
                "Role(s) not permitted to call method; have: "
                .join(', ', map { '"' . $_ . '"' } @{ $opt->{roles} })
                .', need: '
                .join(', ', map { '"' . $_ . '"' } keys %doc_roles)
            );
        }
    }

    foreach my $spec (@{ $message->method->idl->arguments }) {
        my $field = $message->arguments->id($spec->id);
        $class->_validate_parser_message_argument($idl, $opt, $spec, $field);
    }
}

sub _validate_parser_message_argument {
    my ($class, $idl, $opt, $spec, $field) = @_;

    my @docs;
    push @docs, $spec->{doc} if defined $spec->{doc};

    if (defined $field && $spec->type->isa('Thrift::IDL::Type::Custom')) {
        my $ref_object = $idl->object_full_named($spec->type->full_name);
        if ($ref_object && $ref_object->isa('Thrift::IDL::Struct')) {
            my $field_set = $field->value;
            foreach my $child_spec (@{ $ref_object->fields }) {
                my $child_field = $field_set->id($child_spec->id);
                if (! defined $child_field) {
                    next;
                }
                #print "Child spec/field: " . Dumper({ field_set => $field_set, child_field => $child_field, child_spec => $child_spec });
                $class->_validate_parser_message_argument($idl, $opt, $child_spec, $child_field);
            }
        }
        push @docs, $ref_object->{doc} if defined $ref_object->{doc};
    }

    # Create an aggregate doc from the list of @docs
    my $doc = {};
    foreach my $sub (@docs) {
        # Make a copy of it, as I'll be deleting keys from it
        my %sub = %$sub;

        # Overrides
        foreach my $key (qw(description), keys %audit_documentation_flags) {
            next unless defined $sub{$key};
            $doc->{$key} = delete $sub{$key};
        }

        # Hash-based
        foreach my $key (qw(param)) {
            next unless defined $sub{$key};
            $doc->{$key}{$_} = $sub{$key}{$_} foreach keys %{ $sub{$key} };
            delete $sub{$key};
        }

        # Validate
        foreach my $key (keys %{ $sub{validate} }) {
            push @{ $doc->{validate}{$key} }, @{ $sub{validate}{$key} };
        }
        delete $sub{validate};

        # All else is array
        foreach my $key (keys %sub) {
            push @{ $doc->{$key} }, @{ $sub{$key} };
        }
    }

    if (! defined $field && ! $doc->{optional} && ! $spec->optional) {
        MyAPI::InvalidArgument->throw(
            error => "Missing non-optional field ".$spec->name,
            key => $spec->name,
        );
    }

    # Check to see if the user has passed a value for a field which
    # they're not in a role permitted to use.
    if (defined $field && $doc->{role} && $opt->{roles}) {
        # Check to see if there's an overlapping role between what's
        # needed ($doc) and what's provided ($opt)
        my %doc_roles = map { $_ => 1 } @{ $doc->{role} };
        my @overlap = grep { $doc_roles{$_} } @{ $opt->{roles} };
        if (! @overlap) {
            MyAPI::InvalidArgument->throw(
                error => "Role(s) not permitted to use parameter '".$spec->name."'.  Have: "
                    .join(', ', map { '"' . $_ . '"' } @{ $opt->{roles} })
                    .', need: '
                    .join(', ', map { '"' . $_ . '"' } keys %doc_roles),
                key => $spec->name,
            );
        }
    }

    return unless defined $field;

    # Store a reference to the Thrift::IDL::Type spec in the Thrift::Parser::Type object
    # This will give the message arguments direct access to the docs on the spec
    $field->{spec} = $spec;

    my $desc = (defined $field->value ? '"' . $field->value . '"' : 'undef') . ' (' . $spec->name . ')';

    if (defined $field->value
        && $field->isa('Thrift::Parser::Type::string')
        && ! $doc->{utf8}
        && $field->value =~ m{[^\x00-\x7f]}) {
        MyAPI::InvalidArgument->throw(
            error => "String $desc contains > 127 bytes but isn't permitted to have utf8 data",
            key => $field->name, value => $field->value
        );
    }

    return unless $doc->{validate};

    foreach my $key (keys %{ $doc->{validate} }) {
        if ($key eq 'regex' && $field->isa('Thrift::Parser::Type::string')) {
            foreach my $value (@{ $doc->{validate}{$key} }) {
                my $regex;
                if (my ($lq, $body, $rq, $opts) = $value =~ m{^\s* (\S)(.+?)(\S) ([xsmei]*) \s*$}x) {
                    my $left_brackets  = '[{(';
                    my $right_brackets = ']})';
                    if ($lq eq $rq || (
                        index($left_brackets, $lq) >= 0 &&
                        index($left_brackets, $lq) == index($right_brackets, $rq)
                    )) {
                        $regex = eval "qr{$body}$opts";
                        die $@ if $@;
                    }
                }
                if (! $regex) {
                    MyAPI::InvalidSpec->throw(
                        error => "Can't parse regex pattern from '$value'",
                        key => ref($field),
                    );
                }
                if (defined $field->value && $field->value !~ $regex) {
                    MyAPI::InvalidArgument->throw(
                        error => "Argument $desc doesn't pass regex $value",
                        key => $field->name, value => $field->value,
                    );
                }
            }
        }
        elsif ($key eq 'length' && $field->isa('Thrift::Parser::Type::string')) {
            foreach my $value (@{ $doc->{validate}{$key} }) {
                my ($min, $max) = $value =~ /^\s* (\d*) \s*-\s* (\d*) \s*$/x;
                $min = undef unless length $min;
                $max = undef unless length $max;
                if (! defined $min && ! defined $max) {
                    MyAPI::InvalidSpec->throw(
                        error => "Can't parse length range from '$value' (format '\\d* - \\d*'",
                        key => ref($field),
                    );
                }
                my $len = length $field->value;
                if (defined $min && $len < $min) {
                    MyAPI::InvalidArgument->throw(
                        error => "Argument $desc is shorter than permitted ($min)",
                        key => $field->name, value => $field->value,
                    );
                }
                if (defined $max && $len > $max) {
                    MyAPI::InvalidArgument->throw(
                        error => "Argument $desc is longer than permitted ($max)",
                        key => $field->name, value => $field->value,
                    );
                }
            }
        }
        elsif ($key eq 'range' && $field->isa('Thrift::Parser::Type::Number')) {
            foreach my $value (@{ $doc->{validate}{$key} }) {
                my ($min, $max) = $value =~ /^\s* (\d*) \s*-\s* (\d*) \s*$/x;
                $min = undef unless length $min;
                $max = undef unless length $max;
                if (! defined $min && ! defined $max) {
                    MyAPI::InvalidSpec->throw(
                        error => "Can't parse number range from '$value' (format '\\d* - \\d*'",
                        key => ref($field),
                    );
                }
                if (defined $min && $field->value < $min) {
                    MyAPI::InvalidArgument->throw(
                        error => "Argument $desc is smaller than permitted ($min)",
                        key => $field->name, value => $field->value,
                    );
                }
                if (defined $max && $field->value > $max) {
                    MyAPI::InvalidArgument->throw(
                        error => "Argument $desc is longer than permitted ($max)",
                        key => $field->name, value => $field->value,
                    );
                }
            }
        }
        else {
            MyAPI::InvalidSpec->throw(
                error => "Validate key '$key' and field type '".ref($field)."' is not valid",
                key => ref($field),
            );

        }
    }
}

=head2 service_call

Implements a shared service_call() system for both Client and Server alike.  $self needs to implement the following:

  # Self instance keys
  %self = (
      calls => {},
      last_call_id => 0,
      opts => {
      },
      service_parsers => {
          $service_name => Thrift::Parser->new(),
          ...
      },
  );

  # Class accessors
  $self->logger;
  $self->amq;

=cut

sub service_call {
    my ($self, $message, $sequence, $call_opts) = @_;

    # Perform parameter validation
    $self->validate_parser_message($message);

    # Determine the name of the service the message is for
    my ($namespace, $service_name, $method_call) = $message->method =~ m{^(.+)::([^:]+?)::([^:]+)$};
    if (! $service_name) {
        die "Couldn't identify service name from message method class '" . $message->method . "'\n";
    }
    my $queue_name = $call_opts->{queue_name} || $service_name;

    # Uniquely identify the request so I can track failures on the transport
    my $call_id = ++$self->{last_call_id};
    $self->{calls}{$call_id} = {
        message  => $message,
        service  => $service_name,
        queue    => $queue_name,
        sequence => $sequence,
    };

    $message->seqid($call_id);

    my $protocol = $self->get_thrift_protocol('application/x-thrift');
    $message->write($protocol);
    my $payload = $protocol->getTransport->getBuffer;

    # Create a channel

    my $channel = $self->amq->channel(undef, {
        # Don't stop the server connection if I fail
        CascadeFailure => 0,

        # Add an error callback onto the channel such that, if it closes, we are to
        # die inside the user provided sequence, so that the user will be notified
        CloseCallback => sub {
            my $close_reason = shift;
            $sequence->add_action(sub {
                # TODO clear $self of now invalid data; i.e., cleanup after service_call
                die $close_reason . "\n"; # I don't care about the line number
            })->run;
        },
    });

    # Create a reply-to queue, and setup response subscription

    my $reply_queue = $channel->queue(undef, { exclusive => 0 });
    $reply_queue->subscribe(sub { $self->service_response($channel, @_) });

    # Ensure that the service queue exists

    my $service_queue = $channel->queue($queue_name, { passive => 1 });

    # Publish the request

    my $subref_publish_request = sub {
        $service_queue->publish($payload, {
            content_type => 'application/x-thrift',
            reply_to => $reply_queue->name,
            headers => {
                'X-Call-ID' => $call_id, # This is a made up header, but will be passed through
                ($call_opts->{headers} ? (
                %{ $call_opts->{headers} },
                ) : ()),
            },
        });
    };

    if ($call_opts->{status_callback}) {
        my $status_queue = $channel->queue(undef, { exclusive => 0 });
        $status_queue->subscribe(sub {
            my ($payload, $meta) = @_;
            if ($meta->{header_frame}->content_type && $meta->{header_frame}->content_type eq 'application/json') {
                eval { $payload = $jsonxs->decode($payload) };
            }
            $call_opts->{status_callback}->($payload, $meta);
        });
        $status_queue->do_when_created(sub {
            $call_opts->{headers}{'Request-Status-Queue'} = $status_queue->name;
            $reply_queue->do_when_created($subref_publish_request);
        });
    }
    else {
        $reply_queue->do_when_created($subref_publish_request);
    }

    if (my $timeout = $call_opts->{timeout}) {
        $sequence->add_delay($timeout, sub {
            my $seq = shift;
            $seq->failed("Service call to $service_name.".$message->method->name." exceeded timeout $timeout");
        });
    }

    # Start sequence paused; this allows for delays to start and kill sequence if necessary

    $sequence->run();
    $sequence->pause(); # pause after; pausing before will have no effect
    return $sequence;
}

=head2 service_response

Added as a subscription callback in service_call(), above.

=cut

sub service_response {
    my ($self, $channel, $payload, $meta) = @_;

    # Close the channel that was used, or reuse the now idle one (FIXME)
    $channel->close();

    my $content_type = $meta->{header_frame}->content_type;
    my $status_code  = $meta->{header_frame}->headers->{'Status-Code'};

    my $call_id = $meta->{header_frame}->headers->{'X-Call-ID'};
    if (! defined $call_id) {
        $self->logger->error("Received message of type $content_type with status code $status_code but no 'X-Call-ID' header: $payload");
        return;
    }

    my $request = $self->{calls}{$call_id};
    if (! defined $request) {
        $self->logger->error("Received message of type $content_type with status code $status_code but an invalid 'X-Call-ID' header: $payload");
        return;
    }

    # From this point forward, failures should hit the error callback of the sequence

    my $sequence = $request->{sequence};
    if ($sequence->is_finished) {
        $self->logger->error("Sequence that was to handle parsing of service call result is finished; this is probably due to it timing out.  Result will be ignored");
        return;
    }

    $sequence->add_action(sub {
        if ($status_code != 200) {
            die "Error, status $status_code: $payload\n";
        }

        my $protocol = $self->get_thrift_protocol($content_type, \$payload);
        if (! $protocol) {
            die "Invalid content type '$content_type'; no way to handle that\n";
        }

        my $result = $self->{service_parsers}{ $request->{service} }->parse_message($protocol);

        if ($result->type == TMessageType::CALL) {
            die "Unexpected message type 'CALL' in response to service call";
        }
        elsif ($result->type == TMessageType::REPLY) {
            my @ids = @{ $result->arguments->ids };
            if (int @ids != 1) {
                die "Message type 'REPLY' contains more than one argument (ids ".join(', ', @ids).")";
            }
            my $value = $result->arguments->id($ids[0]);
            if ($ids[0] == 0) {
                # normal reply
                $sequence->finished($value);
            }
            else {
                die $value;
            }
        }
        elsif ($result->type == TMessageType::EXCEPTION) {
            die TApplicationException->new(
                $result->arguments->named('message')->value,
                $result->arguments->named('code')->value,
            );
        }
    });

    $sequence->resume();
}

1;
