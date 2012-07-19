use strict;
use warnings;
use FindBin;
use Test::More;
use Test::Deep;
use Time::HiRes qw(gettimeofday);

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
		my ($self) = @_;


	}
}

my $server = MyAPI::Server::ThriftAMQP->create(
	ThriftIDL => $FindBin::Bin . '/thrift/calculator.thrift',
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

isa_ok $server, 'MyAPI::Server::ThriftAMQP';

# Server::ThriftAMQP->handle_amq_message calls child_handle_message; test this

my $message = Services::Calculator::add->compose_message_call(
	num1 => 10,
	num2 => 20,
);

isa_ok $message, 'Thrift::Parser::Message';

my $meta = {};
my $authentication = { success => 1 };
my $time_off_wire  = scalar gettimeofday;

my $method_call = $server->child_handle_message($message, $meta, $authentication, $time_off_wire);

isa_ok $method_call, 'MyAPI::MethodCall';
isa_ok $method_call, 'POE::Component::Sequence';

# Test MethodCall methods

{
	cmp_deeply { $method_call->args() }, {
			num1 => isa('Thrift::Parser::Type::i32'),
			num2 => isa('Thrift::Parser::Type::i32'),
		}, "MethodCall->args('dereference')";
	cmp_deeply { $method_call->args('dereference') }, { num1 => 10, num2 => 20 }, "MethodCall->args('dereference')";
}

done_testing;
