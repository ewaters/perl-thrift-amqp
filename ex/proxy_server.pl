#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;
use lib (
    $FindBin::Bin . '/../lib',
);
use MyAPI::JSONProxy;

MyAPI::JSONProxy->run({
    http_server_port => 8080,
    thrift_idl       => $FindBin::Bin . '/../t/thrift/calculator.thrift',
    ssl_key          => $FindBin::Bin . '/ssl.key',
    ssl_cert         => $FindBin::Bin . '/ssl.crt',
	amq_remote_port  => 5672,
	amq_ssl          => 0,
});
