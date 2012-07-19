package MyAPI::Server::ThriftAMQP::Handler::Object;

use strict;
use warnings;
use base qw(MyAPI::Server::ThriftAMQP::Handler);
use Params::Validate qw(:all);

sub new {
    my $class = shift;

    my %self = validate(@_, {
        object => 1,
        method_prefix => { default => '' },
        method_suffix => { default => '' },
        precall => { default => [] },
        on_complete => { default => [] },
        call => { default => [] },
        catchall => 0,
        throw_on_missing_method => 0,
    });

    if (my @failed = grep { ! $self{object}->can($_) } @{ $self{precall} }) {
        die "Object $self{object} doesn't have precall methods "
            . join(', ', @failed);
    }
    elsif ($self{catchall} && ! $self{object}->can($self{catchall})) {
        die "Object $self{object} doesn't have catchall method $self{catchall}";
    }

    return bless \%self, $class;
}

sub add_actions {
    my ($self, $call) = @_;

    my $object = $self->{object};

    foreach my $method_name (@{ $self->{precall} }) {
        $call->add_action(sub { $object->$method_name($call) });
    }

    my $method_name = $self->{method_prefix} . $call->method->name
        . $self->{method_suffix};

    my $matching_method_name;
    if ($self->{object}->can($method_name)) {
        $matching_method_name = $method_name;
    }
    elsif ($self->{catchall} && $self->{object}->can($self->{catchall})) {
        $matching_method_name = $self->{catchall};
    }
    elsif ($self->{throw_on_missing_method}) {
        $call->add_action(sub { die "Unimplmented method" });
        return;
    }

    $call->add_action(sub { $object->$matching_method_name($call) });
}

sub request_complete {
    my ($self, $call, $text_result) = @_;

    foreach my $method_name (@{ $self->{on_complete} }) {
        if ($self->{object}->can($method_name)) {
            $self->{object}->$method_name($call, $text_result);
        }
        elsif ($self->{throw_on_missing_method}) {
            die "Unimplmented method '$method_name'";
        }
    }
}

1;
