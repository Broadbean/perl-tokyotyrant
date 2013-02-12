#! /usr/bin/perl

use lib qw(./blib/lib ./blib/arch);
use strict;
use warnings;
use Test::More qw(no_plan);
use TokyoTyrant;
$TokyoTyrant::DEBUG = 1;

my @commands = (
                "tcrtest.pl write -tout 3 127.0.0.1 10000",
                "tcrtest.pl read -tout 3 127.0.0.1",
                "tcrtest.pl remove -tout 3 127.0.0.1",
                "tcrtest.pl rcat -tout 3 127.0.0.1 1000",
                "tcrtest.pl rcat -tout 3 -shl 10 127.0.0.1 1000",
                "tcrtest.pl rcat -tout 3 -dai 127.0.0.1 1000",
                "tcrtest.pl rcat -tout 3 -ext put 127.0.0.1 1000",
                "tcrtest.pl misc -tout 3 127.0.0.1 1000",
                );

foreach my $command (@commands){
    my $rv = system("$^X $command >/dev/null");
    ok($rv == 0, $command);
}
