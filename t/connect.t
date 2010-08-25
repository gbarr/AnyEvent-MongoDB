#perl

use strict;
use warnings;
use Test::More;

require_ok('AnyEvent::MongoDB') or exit(1);

ok(my $mdb = AnyEvent::MongoDB->new);

# test the connect method. connect failing is OK as mongod may not be running

SKIP: {
  skip("cannot connect to mongod",1);
}
ok(0);

# test auto_connect

done_testing;

