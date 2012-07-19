package MyAPI::Exceptions;

use strict;
use warnings;

use Exception::Class (
    'MyAPI::Exception',

    'MyAPI::Unauthorized' => {
        isa => 'MyAPI::Exception',
    },

    'MyAPI::InvalidArgument' => {
        isa => 'MyAPI::Exception',
        fields => [ 'key', 'value' ],
    },

    'MyAPI::MissingArgument' => {
        isa => 'MyAPI::InvalidArgument',
    },

    'MyAPI::InvalidSpec' => {
        ida => 'MyAPI::Exception',
        fields => [ 'key' ],
    },

);

1;
