#! /usr/bin/perl -w

#-------------------------------------------------------------------------------------------------
# The test cases of the remote database API
#                                                                Copyright (C) 2006-2010 FAL Labs
# This file is part of Tokyo Tyrant.
# Tokyo Tyrant is free software; you can redistribute it and/or modify it under the terms of
# the GNU Lesser General Public License as published by the Free Software Foundation; either
# version 2.1 of the License or any later version.  Tokyo Tyrant is distributed in the hope
# that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.
# You should have received a copy of the GNU Lesser General Public License along with Tokyo
# Tyrant; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330,
# Boston, MA 02111-1307 USA.
#-------------------------------------------------------------------------------------------------


use lib qw(./blib/lib ./blib/arch);
use strict;
use warnings;
use ExtUtils::testlib;
use Time::HiRes qw(gettimeofday);
use Data::Dumper;
use TokyoTyrant;
$TokyoTyrant::DEBUG = 1;


# main routine
sub main {
    my $rv;
    scalar(@ARGV) >= 1 || usage();
    if($ARGV[0] eq "write"){
        $rv = runwrite();
    } elsif($ARGV[0] eq "read"){
        $rv = runread();
    } elsif($ARGV[0] eq "remove"){
        $rv = runremove();
    } elsif($ARGV[0] eq "rcat"){
        $rv = runrcat();
    } elsif($ARGV[0] eq "misc"){
        $rv = runmisc();
    } elsif($ARGV[0] eq "table"){
        $rv = runtable();
    } else {
        usage();
    }
    return $rv;
}


# print the usage and exit
sub usage {
    printf STDERR ("$0: test cases of the remote database API\n");
    printf STDERR ("\n");
    printf STDERR ("usage:\n");
    printf STDERR ("  $0 write [-port num] [-tout num] [-nr] [-rnd] host rnum\n");
    printf STDERR ("  $0 read [-port num] [-tout num] [-mul num] [-rnd] host rnum\n");
    printf STDERR ("  $0 remove [-port num] [-tout num] [-rnd] host rnum\n");
    printf STDERR ("  $0 rcat [-port num] [-tout num] [-shl num] [-dai|-dad] [-ext name]" .
                   " [-xlg|-xlr] host rnum\n");
    printf STDERR ("  $0 misc [-port num] [-tout num] host rnum\n");
    printf STDERR ("  $0 table [-port num] [-tout num] host rnum\n");
    printf STDERR ("\n");
    exit(1);
}


# parse arguments of write command
sub runwrite {
    my $host = undef;
    my $rnum = undef;
    my $port = 1978;
    my $tout = 0;
    my $nr = 0;
    my $rnd = 0;
    for(my $i = 1; $i < scalar(@ARGV); $i++){
        if(!defined($host) && $ARGV[$i] =~ /^-/){
            if($ARGV[$i] eq "-port"){
                usage() if(++$i > scalar(@ARGV));
                $port = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-tout"){
                usage() if(++$i > scalar(@ARGV));
                $tout = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-nr"){
                $nr = 1;
            } elsif($ARGV[$i] eq "-rnd"){
                $rnd = 1;
            } else {
                usage();
            }
        } elsif(!defined($host)){
            $host = $ARGV[$i];
        } elsif(!defined($rnum)){
            $rnum = TokyoTyrant::atoi($ARGV[$i]);
        } else {
            usage();
        }
    }
    usage() if(!defined($host) || !defined($rnum) || $rnum < 1);
    my $rv = procwrite($host, $port, $tout, $rnum, $nr, $rnd);
    return $rv;
}


# parse arguments of read command
sub runread {
    my $host = undef;
    my $port = 1978;
    my $tout = 0;
    my $mul = 0;
    my $rnd = 0;
    for(my $i = 1; $i < scalar(@ARGV); $i++){
        if(!defined($host) && $ARGV[$i] =~ /^-/){
            if($ARGV[$i] eq "-port"){
                usage() if(++$i > scalar(@ARGV));
                $port = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-tout"){
                usage() if(++$i > scalar(@ARGV));
                $tout = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-mul"){
                usage() if(++$i > scalar(@ARGV));
                $mul = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-rnd"){
                $rnd = 1;
            } else {
                usage();
            }
        } elsif(!defined($host)){
            $host = $ARGV[$i];
        } else {
            usage();
        }
    }
    usage() if(!defined($host));
    my $rv = procread($host, $port, $tout, $mul, $rnd);
    return $rv;
}


# parse arguments of remove command
sub runremove {
    my $host = undef;
    my $port = 1978;
    my $tout = 0;
    my $rnd = 0;
    for(my $i = 1; $i < scalar(@ARGV); $i++){
        if(!defined($host) && $ARGV[$i] =~ /^-/){
            if($ARGV[$i] eq "-port"){
                usage() if(++$i > scalar(@ARGV));
                $port = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-tout"){
                usage() if(++$i > scalar(@ARGV));
                $tout = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-rnd"){
                $rnd = 1;
            } else {
                usage();
            }
        } elsif(!defined($host)){
            $host = $ARGV[$i];
        } else {
            usage();
        }
    }
    usage() if(!defined($host));
    my $rv = procremove($host, $port, $tout, $rnd);
    return $rv;
}


# parse arguments of rcat command
sub runrcat {
    my $host = undef;
    my $rnum = undef;
    my $port = 1978;
    my $tout = 0;
    my $shl = 0;
    my $dai = 0;
    my $dad = 0;
    my $ext = undef;
    my $xopts = 0;
    for(my $i = 1; $i < scalar(@ARGV); $i++){
        if(!defined($host) && $ARGV[$i] =~ /^-/){
            if($ARGV[$i] eq "-port"){
                usage() if(++$i > scalar(@ARGV));
                $port = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-tout"){
                usage() if(++$i > scalar(@ARGV));
                $tout = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-shl"){
                usage() if(++$i > scalar(@ARGV));
                $shl = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-dai"){
                $dai = 1;
            } elsif($ARGV[$i] eq "-dad"){
                $dad = 1;
            } elsif($ARGV[$i] eq "-ext"){
                usage() if(++$i > scalar(@ARGV));
                $ext = $ARGV[$i];
            } elsif($ARGV[$i] eq "-xlr"){
                $xopts |= TokyoTyrant::RDB->XOLCKREC;
            } elsif($ARGV[$i] eq "-xlg"){
                $xopts |= TokyoTyrant::RDB->XOLCKGLB;
            } else {
                usage();
            }
        } elsif(!defined($host)){
            $host = $ARGV[$i];
        } elsif(!defined($rnum)){
            $rnum = TokyoTyrant::atoi($ARGV[$i]);
        } else {
            usage();
        }
    }
    usage() if(!defined($host) || !defined($rnum) || $rnum < 1);
    my $rv = procrcat($host, $port, $tout, $rnum, $shl, $dai, $dad, $ext, $xopts);
    return $rv;
}


# parse arguments of misc command
sub runmisc {
    my $host = undef;
    my $rnum = undef;
    my $port = 1978;
    my $tout = 0;
    for(my $i = 1; $i < scalar(@ARGV); $i++){
        if(!defined($host) && $ARGV[$i] =~ /^-/){
            if($ARGV[$i] eq "-port"){
                usage() if(++$i > scalar(@ARGV));
                $port = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-tout"){
                usage() if(++$i > scalar(@ARGV));
                $tout = TokyoTyrant::atoi($ARGV[$i]);
            } else {
                usage();
            }
        } elsif(!defined($host)){
            $host = $ARGV[$i];
        } elsif(!defined($rnum)){
            $rnum = TokyoTyrant::atoi($ARGV[$i]);
        } else {
            usage();
        }
    }
    usage() if(!defined($host) || !defined($rnum) || $rnum < 1);
    my $rv = procmisc($host, $port, $tout, $rnum);
    return $rv;
}


# parse arguments of table command
sub runtable {
    my $host = undef;
    my $rnum = undef;
    my $port = 1978;
    my $tout = 0;
    for(my $i = 1; $i < scalar(@ARGV); $i++){
        if(!defined($host) && $ARGV[$i] =~ /^-/){
            if($ARGV[$i] eq "-port"){
                usage() if(++$i > scalar(@ARGV));
                $port = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-tout"){
                usage() if(++$i > scalar(@ARGV));
                $tout = TokyoTyrant::atoi($ARGV[$i]);
            } else {
                usage();
            }
        } elsif(!defined($host)){
            $host = $ARGV[$i];
        } elsif(!defined($rnum)){
            $rnum = TokyoTyrant::atoi($ARGV[$i]);
        } else {
            usage();
        }
    }
    usage() if(!defined($host) || !defined($rnum) || $rnum < 1);
    my $rv = proctable($host, $port, $tout, $rnum);
    return $rv;
}


# print error message of remote database
sub eprint {
    my $rdb = shift;
    my $func = shift;
    my $ecode = $rdb->ecode();
    printf STDERR ("%s: %s: error: %d: %s\n", $0, $func, $ecode, $rdb->errmsg($ecode));
}


# perform write command
sub procwrite {
    my $host = shift;
    my $port = shift;
    my $tout = shift;
    my $rnum = shift;
    my $nr = shift;
    my $rnd = shift;
    printf("<Writing Test>\n  host=%s  port=%d  tout=%d  rnum=%d  nr=%d  rnd=%d\n\n",
           $host, $port, $tout, $rnum, $nr, $rnd);
    my $err = 0;
    my $stime = gettimeofday();
    my $rdb = TokyoTyrant::RDB->new();
    if(!$rdb->open($host, $port, $tout)){
        eprint($rdb, "open");
        $err = 1;
    }
    if(!$rnd && !$rdb->vanish()){
        eprint($rdb, "vanish");
        $err = 1;
    }
    for(my $i = 1; $i <= $rnum; $i++){
        my $buf = sprintf("%08d", $rnd ? int(rand($rnum)) + 1 : $i);
        if($nr){
            if(!$rdb->putnr($buf, $buf)){
                eprint($rdb, "putnr");
                $err = 1;
                last;
            }
        } else {
            if(!$rdb->put($buf, $buf)){
                eprint($rdb, "put");
                $err = 1;
                last;
            }
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("record number: %llu\n", $rdb->rnum());
    printf("size: %llu\n", $rdb->size());
    if(!$rdb->close()){
        eprint($rdb, "close");
        $err = 1;
    }
    printf("time: %.3f\n", gettimeofday() - $stime);
    printf("%s\n\n", $err ? "error" : "ok");
    return $err ? 1 : 0;
}


# perform read command
sub procread {
    my $host = shift;
    my $port = shift;
    my $tout = shift;
    my $mul = shift;
    my $rnd = shift;
    printf("<Reading Test>\n  host=%s  port=%d  tout=%d  mul=%d  rnd=%d\n\n",
           $host, $port, $tout, $mul, $rnd);
    my $err = 0;
    my $stime = gettimeofday();
    my $rdb = TokyoTyrant::RDB->new();
    if(!$rdb->open($host, $port, $tout)){
        eprint($rdb, "open");
        $err = 1;
    }
    my %recs;
    my $rnum = $rdb->rnum();
    for(my $i = 1; $i <= $rnum; $i++){
        my $buf = sprintf("%08d", $rnd ? int(rand($rnum)) + 1 : $i);
        if($mul > 1){
            $recs{$buf} = "";
            if($i % $mul == 0){
                if($rdb->mget(\%recs) < 0){
                    eprint($rdb, "mget");
                    $err = 1;
                    last;
                }
                %recs = ();
            }
        } else {
            if(!$rdb->get($buf) && !$rnd){
                eprint($rdb, "get");
                $err = 1;
                last;
            }
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("record number: %llu\n", $rdb->rnum());
    printf("size: %llu\n", $rdb->size());
    if(!$rdb->close()){
        eprint($rdb, "close");
        $err = 1;
    }
    printf("time: %.3f\n", gettimeofday() - $stime);
    printf("%s\n\n", $err ? "error" : "ok");
    return $err ? 1 : 0;
}


# perform remove command
sub procremove {
    my $host = shift;
    my $port = shift;
    my $tout = shift;
    my $rnd = shift;
    printf("<Removing Test>\n  host=%s  port=%d  tout=%d  rnd=%d\n\n",
           $host, $port, $tout, $rnd);
    my $err = 0;
    my $stime = gettimeofday();
    my $rdb = TokyoTyrant::RDB->new();
    if(!$rdb->open($host, $port, $tout)){
        eprint($rdb, "open");
        $err = 1;
    }
    my $rnum = $rdb->rnum();
    for(my $i = 1; $i <= $rnum; $i++){
        my $buf = sprintf("%08d", $rnd ? int(rand($rnum)) + 1 : $i);
        if(!$rdb->out($buf) && !$rnd){
            eprint($rdb, "out");
            $err = 1;
            last;
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("record number: %llu\n", $rdb->rnum());
    printf("size: %llu\n", $rdb->size());
    if(!$rdb->close()){
        eprint($rdb, "close");
        $err = 1;
    }
    printf("time: %.3f\n", gettimeofday() - $stime);
    printf("%s\n\n", $err ? "error" : "ok");
    return $err ? 1 : 0;
}


# perform rcat command
sub procrcat {
    my $host = shift;
    my $port = shift;
    my $tout = shift;
    my $rnum = shift;
    my $shl = shift;
    my $dai = shift;
    my $dad = shift;
    my $ext = shift;
    my $xopts = shift;
    printf("<Random Concatenating Test>\n  host=%s  port=%d  tout=%d  rnum=%d" .
           "  shl=%d  dai=%d  dad=%d  ext=%s  xopts=%d\n\n",
           $host, $port, $tout, $rnum, $shl, $dai, $dad, $ext ? $ext : "", $xopts);
    my $pnum = $rnum / 5 + 1;
    my $err = 0;
    my $stime = gettimeofday();
    my $rdb = TokyoTyrant::RDB->new();
    if(!$rdb->open($host, $port, $tout)){
        eprint($rdb, "open");
        $err = 1;
    }
    if(!$rdb->vanish()){
        eprint($rdb, "vanish");
        $err = 1;
    }
    for(my $i = 1; $i <= $rnum; $i++){
        my $buf = sprintf("%08d", int(rand($pnum)) + 1);
        if($shl > 0){
            if(!$rdb->putshl($buf, $buf, $shl)){
                eprint($rdb, "putshl");
                $err = 1;
                last;
            }
        } elsif($dai){
            if(!defined($rdb->addint($buf, 1))){
                eprint($rdb, "addint");
                $err = 1;
                last;
            }
        } elsif($dad){
            if(!defined($rdb->adddouble($buf, 1))){
                eprint($rdb, "adddouble");
                $err = 1;
                last;
            }
        } elsif(defined($ext)){
              if(!$rdb->ext($ext, $buf, $buf, $xopts) && $rdb->ecode() != $rdb->EMISC){
                eprint($rdb, "ext");
                $err = 1;
                last;
            }
        } else {
            if(!$rdb->putcat($buf, $buf)){
                eprint($rdb, "putcat");
                $err = 1;
                last;
            }
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("record number: %llu\n", $rdb->rnum());
    printf("size: %llu\n", $rdb->size());
    if(!$rdb->close()){
        eprint($rdb, "close");
        $err = 1;
    }
    printf("time: %.3f\n", gettimeofday() - $stime);
    printf("%s\n\n", $err ? "error" : "ok");
    return $err ? 1 : 0;
}


# perform misc command
sub procmisc {
    my $host = shift;
    my $port = shift;
    my $tout = shift;
    my $rnum = shift;
    printf("<Miscellaneous Test>\n  host=%s  port=%d  tout=%d  rnum=%d\n\n",
           $host, $port, $tout, $rnum);
    my $err = 0;
    my $stime = gettimeofday();
    my $rdb = TokyoTyrant::RDB->new();
    if(!$rdb->open($host, $port, $tout)){
        eprint($rdb, "open");
        $err = 1;
    }
    if(!$rdb->vanish()){
        eprint($rdb, "vanish");
        $err = 1;
    }
    printf("writing:\n");
    for(my $i = 1; $i <= $rnum; $i++){
        my $buf = sprintf("%08d", $i);
        if(int(rand(10)) > 0){
            if(!$rdb->putkeep($buf, $buf)){
                eprint($rdb, "putkeep");
                $err = 1;
                last;
            }
        } else {
            if(!$rdb->putnr($buf, $buf)){
                eprint($rdb, "putnr");
                $err = 1;
                last;
            }
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("reading:\n");
    for(my $i = 1; $i <= $rnum; $i++){
        my $kbuf = sprintf("%08d", $i);
        my $vbuf = $rdb->get($kbuf);
        if(!defined($vbuf)){
            eprint($rdb, "get");
            $err = 1;
            last;
        }
        if($vbuf ne $kbuf){
            eprint($rdb, "(validation)");
            $err = 1;
            last;
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    if($rdb->rnum() != $rnum){
        eprint($rdb, "rnum");
        $err = 1;
    }
    printf("random writing:\n");
    for(my $i = 1; $i <= $rnum; $i++){
        my $kbuf = sprintf("%08d", int(rand($rnum)) + 1);
        my $vbuf = '*' x int(rand(32));
        if(!$rdb->put($kbuf, $vbuf)){
            eprint($rdb, "put");
            $err = 1;
            last;
        }
        my $rbuf = $rdb->get($kbuf);
        if(!defined($rbuf)){
            eprint($rdb, "get");
            $err = 1;
            last;
        }
        if($rbuf ne $vbuf){
            eprint($rdb, "(validation)");
            $err = 1;
            last;
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("random erasing:\n");
    for(my $i = 1; $i <= $rnum; $i++){
        my $kbuf = sprintf("%08d", int(rand($rnum)) + 1);
        if(!$rdb->out($kbuf) && $rdb->ecode() != $rdb->ENOREC){
            eprint($rdb, "out");
            $err = 1;
            last;
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("script extension calling:\n");
    for(my $i = 1; $i <= $rnum; $i++){
        my $buf = sprintf("(%d)", int(rand($rnum)) + 1);
        my $name = "put";
        my $rnd = int(rand(7));
        if($rnd == 1){
            $name = "putkeep";
        } elsif($rnd == 2){
            $name = "putcat";
        } elsif($rnd == 3){
            $name = "out";
        } elsif($rnd == 4){
            $name = "get";
        } elsif($rnd == 5){
            $name = "iterinit";
        } elsif($rnd == 6){
            $name = "iternext";
        }
        my $xbuf = $rdb->ext($name, $buf, $buf);
        if(!defined($xbuf) && $rdb->ecode() != $rdb->EMISC){
            eprint($rdb, "ext");
            $err = 1;
            last;
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("checking iterator:\n");
    if(!$rdb->iterinit()){
        eprint($rdb, "iterinit");
        $err = 1;
    }
    my $inum = 0;
    while(defined(my $key = $rdb->iternext())){
        $inum++;
        my $value = $rdb->get($key);
        if(!defined($value)){
            eprint($rdb, "get");
            $err = 1;
        }
        if($rnum > 250 && $inum % ($rnum / 250) == 0){
            print('.');
            if($inum == $rnum || $inum % ($rnum / 10) == 0){
                printf(" (%08d)\n", $inum);
            }
        }
    }
    printf(" (%08d)\n", $inum) if($rnum > 250);
    if($rdb->ecode() != $rdb->ENOREC || $inum != $rdb->rnum()){
        eprint($rdb, "(validation)");
        $err = 1;
    }
    my $keys = $rdb->fwmkeys("0", 10);
    if($rdb->rnum() >= 10 && scalar(@$keys) != 10){
        eprint($rdb, "fwmkeys");
        $err = 1;
    }
    printf("checking counting:\n");
    for(my $i = 1; $i <= $rnum; $i++){
        my $buf = sprintf("[%d]", int(rand($rnum)));
        if(int(rand(2)) == 0){
            if(!$rdb->addint($buf, 123) && $rdb->ecode() != $rdb->EKEEP){
                eprint($rdb, "addint");
                $err = 1;
                last;
            }
        } else {
            if(!$rdb->adddouble($buf, 123.456) && $rdb->ecode() != $rdb->EKEEP){
                eprint($rdb, "adddouble");
                $err = 1;
                last;
            }
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("checking versatile functions:\n");
    my @args = ();
    for(my $i = 1; $i <= $rnum; $i++){
        if(int(rand(10)) == 0){
            my $rnd = int(rand(3));
            my $name = "putlist";
            if($rnd == 1){
                $name = "outlist";
            } elsif($rnd == 2){
                $name = "getlist";
            }
            my $res = $rdb->misc($name, \@args);
            if(!defined($res)){
                eprint($rdb, "misc");
                $err = 1;
                last;
            }
            @args = ();
        } else {
            my $buf = sprintf("(%d)", int(rand($rnum)));
            push(@args, $buf);
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    if(!defined($rdb->stat())){
        eprint($rdb, "stat");
        $err = 1;
    }
    if(!$rdb->sync()){
        eprint($rdb, "sync");
        $err = 1;
    }
    if(!$rdb->optimize()){
        eprint($rdb, "optimize");
        $err = 1;
    }
    if(!$rdb->vanish()){
        eprint($rdb, "vanish");
        $err = 1;
    }
    printf("record number: %llu\n", $rdb->rnum());
    printf("size: %llu\n", $rdb->size());
    if(!$rdb->close()){
        eprint($rdb, "close");
        $err = 1;
    }
    printf("checking tied updating:\n");
    my %hash;
    if(!tie(%hash, "TokyoTyrant::RDB", $host, $port)){
        eprint($rdb, "tie");
        $err = 1;
    }
    for(my $i = 1; $i <= $rnum; $i++){
        my $buf = sprintf("[%d]", int(rand($rnum)));
        my $rnd = int(rand(4));
        if($rnd == 0){
            $hash{$buf} = $buf;
        } elsif($rnd == 1){
            my $value = $hash{$buf};
        } elsif($rnd == 2){
            my $res = exists($hash{$buf});
        } elsif($rnd == 3){
            delete($hash{$buf});
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("checking tied iterator:\n");
    $inum = 0;
    while(my ($key, $value) = each(%hash)){
        $inum++;
        if($rnum > 250 && $inum % ($rnum / 250) == 0){
            print('.');
            if($inum == $rnum || $inum % ($rnum / 10) == 0){
                printf(" (%08d)\n", $inum);
            }
        }
    }
    printf(" (%08d)\n", $inum) if($rnum > 250);
    %hash = ();
    untie(%hash);
    printf("time: %.3f\n", gettimeofday() - $stime);
    printf("%s\n\n", $err ? "error" : "ok");
    return $err ? 1 : 0;
}


# perform table command
sub proctable {
    my $host = shift;
    my $port = shift;
    my $tout = shift;
    my $rnum = shift;
    printf("<Table Extension Test>\n  host=%s  port=%d  tout=%d  rnum=%d\n\n",
           $host, $port, $tout, $rnum);
    my $err = 0;
    my $stime = gettimeofday();
    my $rdb = TokyoTyrant::RDBTBL->new();
    if(!$rdb->open($host, $port, $tout)){
        eprint($rdb, "open");
        $err = 1;
    }
    if(!$rdb->vanish()){
        eprint($rdb, "vanish");
        $err = 1;
    }
    if(!$rdb->setindex("", $rdb->ITDECIMAL)){
        eprint($rdb, "setindex");
        $err = 1;
    }
    if(!$rdb->setindex("str", $rdb->ITLEXICAL)){
        eprint($rdb, "setindex");
        $err = 1;
    }
    if(!$rdb->setindex("num", $rdb->ITDECIMAL)){
        eprint($rdb, "setindex");
        $err = 1;
    }
    if(!$rdb->setindex("type", $rdb->ITDECIMAL)){
        eprint($rdb, "setindex");
        $err = 1;
    }
    if(!$rdb->setindex("flag", $rdb->ITTOKEN)){
        eprint($rdb, "setindex");
        $err = 1;
    }
    if(!$rdb->setindex("text", $rdb->ITQGRAM)){
        eprint($rdb, "setindex");
        $err = 1;
    }
    printf("writing:\n");
    for(my $i = 1; $i <= $rnum; $i++){
        my $id = $rdb->genuid();
        my $cols = {
            str => $id,
            num => int(rand($id)) + 1,
            type => int(rand(32)) + 1,
        };
        my $vbuf = "";
        my $num = int(rand(5));
        my $pt = 0;
        for(my $j = 0; $j < $num; $j++){
            $pt += int(rand(5)) + 1;
            $vbuf .= "," if(length($vbuf) > 0);
            $vbuf .= $pt;
        }
        if(length($vbuf) > 0){
            $cols->{flag} = $vbuf;
            $cols->{text} = $vbuf;
        }
        if(!$rdb->put($id, $cols)){
            eprint($rdb, "put");
            $err = 1;
            last;
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("reading:\n");
    for(my $i = 1; $i <= $rnum; $i++){
        if(!$rdb->get($i)){
            eprint($rdb, "get");
            $err = 1;
            last;
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    my $recs = { 1 => "", 2 => "", 3 => "", 4 => "" };
    if($rdb->mget($recs) != 4 || scalar(keys(%$recs)) != 4 || $recs->{1}{"str"} ne "1"){
        eprint($rdb, "mget");
        $err = 1;
    }
    printf("removing:\n");
    for(my $i = 1; $i <= $rnum; $i++){
        if(int(rand(2)) == 0 && !$rdb->out($i)){
            eprint($rdb, "out");
            $err = 1;
            last;
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    printf("searching:\n");
    my $qry = TokyoTyrant::RDBQRY->new($rdb);
    my @names = ( "", "str", "num", "type", "flag", "text", "c1" );
    my @ops = ( $qry->QCSTREQ, $qry->QCSTRINC, $qry->QCSTRBW, $qry->QCSTREW, $qry->QCSTRAND,
                $qry->QCSTROR, $qry->QCSTROREQ, $qry->QCSTRRX, $qry->QCNUMEQ, $qry->QCNUMGT,
                $qry->QCNUMGE, $qry->QCNUMLT, $qry->QCNUMLE, $qry->QCNUMBT, $qry->QCNUMOREQ );
    my @ftsops = ( $qry->QCFTSPH, $qry->QCFTSAND, $qry->QCFTSOR, $qry->QCFTSEX );
    my @types = ( $qry->QOSTRASC, $qry->QOSTRDESC, $qry->QONUMASC, $qry->QONUMDESC );
    for(my $i = 1; $i <= $rnum; $i++){
        $qry = TokyoTyrant::RDBQRY->new($rdb) if(int(rand(10)) > 0);
        my $cnum = int(rand(4));
        for(my $j = 0; $j < $cnum; $j++){
            my $name = $names[int(rand(scalar(@names)))];
            my $op = $ops[int(rand(scalar(@ops)))];
            $op = $ftsops[int(rand(scalar(@ftsops)))] if(int(rand(10)) == 0);
            $op |= $qry->QCNEGATE if(int(rand(20)) == 0);
            $op |= $qry->QCNOIDX if(int(rand(20)) == 0);
            my $expr = int(rand($i));
            $expr .= "," . int(rand($i)) if(int(rand(10)) == 0);
            $expr .= "," . int(rand($i)) if(int(rand(10)) == 0);
            $qry->addcond($name, $op, $expr);
        }
        if(int(rand(3)) != 0){
            my $name = $names[int(rand(scalar(@names)))];
            my $type = $types[int(rand(scalar(@types)))];
            $qry->setorder($name, $type);
        }
        if(int(rand(20)) == 0){
            $qry->setlimit(10, int(rand(10)));
            my $res = $qry->searchget();
            foreach my $cols (@$res){
                my $pkey = $cols->{""};
                my $str = $cols->{"str"};
                if(!defined($pkey) || !defined($str) || $pkey ne $str){
                    eprint($rdb, "searchget");
                    $err = 1;
                    last;
                }
            }
            if($qry->searchcount() != scalar(@$res)){
                eprint($rdb, "searchcount");
                $err = 1;
                last;
            }
            my $onum = $rdb->rnum();
            if(!$qry->searchout()){
                eprint($rdb, "searchout");
                $err = 1;
                last;
            }
            if($rdb->rnum() != $onum - scalar(@$res)){
                eprint($rdb, "(validation)");
                $err = 1;
                last;
            }
        } elsif(int(rand(20)) == 0){
            $qry->setlimit(10);
            my $res = $qry->metasearch([ $qry ], $qry->MSUNION + int(rand(3)));
        } else {
            $qry->setlimit(int(rand($i)), int(rand(10))) if(int(rand(3)) != 0);
            my $res = $qry->search();
        }
        if($rnum > 250 && $i % ($rnum / 250) == 0){
            print('.');
            if($i == $rnum || $i % ($rnum / 10) == 0){
                printf(" (%08d)\n", $i);
            }
        }
    }
    my $pkey = int(rand($rnum));
    $rdb->put($pkey, { "name" => "mikio", "birth" => "19780211" });
    if($rdb->putkeep($pkey, {})){
        eprint($rdb, "putkeep");
        $err = 1;
    } elsif($rdb->ecode() != $rdb->EKEEP){
        eprint($rdb, "putkeep");
        $err = 1;
    }
    if(!$rdb->get($pkey)){
        eprint($rdb, "get");
        $err = 1;
    }
    if(!$rdb->out($pkey)){
        eprint($rdb, "out");
        $err = 1;
    }
    if($rdb->get($pkey)){
        eprint($rdb, "get");
        $err = 1;
    } elsif($rdb->ecode() != $rdb->ENOREC){
        eprint($rdb, "get");
        $err = 1;
    }
    if($rdb->out($pkey)){
        eprint($rdb, "out");
        $err = 1;
    } elsif($rdb->ecode() != $rdb->ENOREC){
        eprint($rdb, "out");
        $err = 1;
    }
    printf("record number: %llu\n", $rdb->rnum());
    printf("size: %llu\n", $rdb->size());
    if(!$rdb->close()){
        eprint($rdb, "close");
        $err = 1;
    }
    printf("time: %.3f\n", gettimeofday() - $stime);
    printf("%s\n\n", $err ? "error" : "ok");
    return $err ? 1 : 0;
}


# execute main
$| = 1;
$0 =~ s/.*\///;
exit(main());



# END OF FILE
