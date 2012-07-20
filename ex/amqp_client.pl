#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;
use MyAPI::Client::ThriftAMQP;

my $client = MyAPI::Client::ThriftAMQP->new(
	ThriftIDL => $FindBin::Bin . '/../t/thrift/calculator.thrift',

	Debug => 1,
	
	# AMQP connection parameters
	RemoteAddress => '127.0.0.1',
	RemotePort    => 5672,
	Username      => 'guest',
	Password      => 'guest',
	SSL           => 0,
);

my $message = Services::Calculator::add->compose_message_call(
	num1 => 10,
	num2 => 20,
);

my $sequence = $client->service_call_sequence($message);

$sequence->add_callback(sub {
	my ($sequence, $result) = @_;
	$client->logger->info("Successful method create: got $result");
});

$sequence->add_error_callback(sub {
	my ($sequence, $error) = @_;
	$client->logger->error("An error has occurred: $error");
});

$sequence->add_finally_callback(sub {
	$client->stop;
});

$client->run;
