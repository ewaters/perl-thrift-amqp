#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;

use MyAPI::Server::ThriftAMQP;

{
	package My::Calculator;

	use strict;
	use warnings;

	sub new {
		my $class = shift;
		return bless {}, $class;
	}

	sub method_add {
		my ($self, $method) = @_;
		my %args = $method->args('dereference');

		$method->set_result($args{num1} + $args{num2});
	}
}

my $server = MyAPI::Server::ThriftAMQP->create(
	ThriftIDL => $FindBin::Bin . '/../t/thrift/calculator.thrift',
	Service   => 'Calculator',
	Handlers  => [{
		object => My::Calculator->new(),
		method_prefix => 'method_',
		throw_on_missing_method => 1,
	}],

	Debug => 1,

	# AMQP connection parameters
	RemoteAddress => '127.0.0.1',
	RemotePort    => 5672,
	Username      => 'guest',
	Password      => 'guest',
	SSL           => 0,
);

$server->run();
