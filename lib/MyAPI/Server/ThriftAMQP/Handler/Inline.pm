package MyAPI::Server::ThriftAMQP::Handler::Inline;

use strict;
use warnings;
use base qw(MyAPI::Server::ThriftAMQP::Handler);
use Params::Validate qw(:all);

sub new {
    my $class = shift;

    my %self = validate(@_, {
        inline => 1,
        methods => { optional => 1, type => HASHREF },
        catchall => { optional => 1, type => CODEREF },
    });

    return bless \%self, $class;
}

sub add_actions {
    my ($self, $call) = @_;

    my $method_name = $call->method->name;

    my $matching_subref;

    while (my ($key, $subref) = each %{ $self->{methods} }) {
        next if $key ne $method_name;
        $matching_subref = $subref;
        last;
    }
    $matching_subref ||= $self->{catchall} if $self->{catchall};

    return unless $matching_subref;

    $call->add_action($matching_subref);
}

1;
