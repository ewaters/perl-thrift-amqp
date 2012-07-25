use Module::Build;

my $build = Module::Build->new(
	module_name => 'Thrift::IDL',
	license     => 'perl',
	sign        => 1,
	configure_requires => {
		'Module::Build' => 0.4,
	},
	build_requires => {
		'Test::More' => 0,
		'Test::Deep' => 0,
	},
	requires => {
		'Parse::RecDescent' => 0,
		'Class::Accessor'   => 0,
		'Class::Accessor::Grouped' => 0,
		'Class::Data::Accessor' => 0,
		'Class::ISA'        => 0,
		'POE::Component::Client::AMQP' => 0,
		'Exception::Class'  => 0,
		'JSON::XS'          => 0,

		# Thrfit::BinaryProtocol
		'Bit::Vector'       => 0, 

		# AMQP debug
		'YAML::XS' => 0,
		'Term::ANSIColor' => 0,

		# PoCo::Sequence
		'Class::MethodMaker' => 0,

		# MyAPI::Provider
		# get_logger
		'Log::Dispatch::Syslog' => 0,
		'Net::Address::IP::Local' => 0,
		'Log::Log4perl' => 0,

		# MyAPI::Server::ThriftAMQP
		'Net::IP' => 0,

		# PoCo::Server::HTTPs & proxy
		'LWP'        => 0,
		'Data::UUID' => 0,
		'POE::Component::Server::HTTP' => 0,
		'IO::Socket::SSL' => 0,
	},
	create_makefile_pl => 'small',
);

$build->create_build_script;

