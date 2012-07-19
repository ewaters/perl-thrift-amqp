package MyAPI::Server::ThriftAMQP::Handler::Package;

use strict;
use warnings;
use base qw(MyAPI::Server::ThriftAMQP::Handler);
use Params::Validate qw(:all);

sub new {
    my $class = shift;

    my %self = validate(@_, {
        'package' => 1,
        methods => { type => ARRAYREF },
    });

    return bless \%self, $class;
}

1;
