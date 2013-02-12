#! /usr/bin/perl -w

#-------------------------------------------------------------------------------------------------
# The test cases of Tokyo Usherette
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
use TokyoTyrant;
$TokyoTyrant::DEBUG = 1;

use constant {
    DEFWNUM => 20,
    VALRATIO => 10.0,
    DEFPNUM => 2,
};


# main routine
sub main {
    my $rv;
    scalar(@ARGV) >= 1 || usage();
    if($ARGV[0] eq "write"){
        $rv = runwrite()
    } elsif($ARGV[0] eq "read"){
        $rv = runread()
    } elsif($ARGV[0] eq "remove"){
        $rv = runremove()
    } else {
        usage();
    }
    return $rv;
}


# print the usage and exit
sub usage {
    printf STDERR ("$0: client of Tokyo Usherette\n");
    printf STDERR ("\n");
    printf STDERR ("usage:\n");
    printf STDERR ("  $0 write [-port num] [-x] host rnum [wnum]\n");
    printf STDERR ("  $0 read [-port num] [-x] host rnum [pnum]\n");
    printf STDERR ("  $0 remove [-port num] [-x] host rnum [wnum]\n");
    printf STDERR ("\n");
    exit(1);
}


# print error message of remote database
sub eprint {
    my $rdb = shift;
    my $func = shift;
    my $ecode = $rdb->ecode();
    printf STDERR ("%s: %s: error: %d: %s\n", $0, $func, $ecode, $rdb->errmsg($ecode));
}


# parse arguments of write command
sub runwrite {
    my $host = undef;
    my $rnum = undef;
    my $wnum = undef;
    my $port = 1978;
    my $xb = 0;
    for(my $i = 1; $i < scalar(@ARGV); $i++){
        if(!defined($host) && $ARGV[$i] =~ /^-/){
            if($ARGV[$i] eq "-port"){
                usage() if(++$i > scalar(@ARGV));
                $port = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-x"){
                $xb = 1;
            } else {
                usage();
            }
        } elsif(!defined($host)){
            $host = $ARGV[$i];
        } elsif(!defined($rnum)){
            $rnum = TokyoTyrant::atoi($ARGV[$i]);
        } elsif(!defined($wnum)){
            $wnum = TokyoTyrant::atoi($ARGV[$i]);
        } else {
            usage();
        }
    }
    usage() if(!defined($host) || !defined($rnum) || $rnum < 1);
    $wnum = DEFWNUM if(!defined($wnum) || $wnum < 1);
    my $rv = procwrite($host, $port, $rnum, $wnum, $xb);
    return $rv;
}


# parse arguments of read command
sub runread {
    my $host = undef;
    my $rnum = undef;
    my $pnum = undef;
    my $port = 1978;
    my $xb = 0;
    for(my $i = 1; $i < scalar(@ARGV); $i++){
        if(!defined($host) && $ARGV[$i] =~ /^-/){
            if($ARGV[$i] eq "-port"){
                usage() if(++$i > scalar(@ARGV));
                $port = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-x"){
                $xb = 1;
            } else {
                usage();
            }
        } elsif(!defined($host)){
            $host = $ARGV[$i];
        } elsif(!defined($rnum)){
            $rnum = TokyoTyrant::atoi($ARGV[$i]);
        } elsif(!defined($pnum)){
            $pnum = TokyoTyrant::atoi($ARGV[$i]);
        } else {
            usage();
        }
    }
    usage() if(!defined($host) || !defined($rnum) || $rnum < 1);
    $pnum = DEFPNUM if(!defined($pnum) || $pnum < 1);
    my $rv = procread($host, $port, $rnum, $pnum, $xb);
    return $rv;
}


# parse arguments of remove command
sub runremove {
    my $host = undef;
    my $rnum = undef;
    my $wnum = undef;
    my $port = 1978;
    my $xb = 1;
    for(my $i = 1; $i < scalar(@ARGV); $i++){
        if(!defined($host) && $ARGV[$i] =~ /^-/){
            if($ARGV[$i] eq "-port"){
                usage() if(++$i > scalar(@ARGV));
                $port = TokyoTyrant::atoi($ARGV[$i]);
            } elsif($ARGV[$i] eq "-x"){
                $xb = 1;
            } else {
                usage();
            }
        } elsif(!defined($host)){
            $host = $ARGV[$i];
        } elsif(!defined($rnum)){
            $rnum = TokyoTyrant::atoi($ARGV[$i]);
        } elsif(!defined($wnum)){
            $wnum = TokyoTyrant::atoi($ARGV[$i]);
        } else {
            usage();
        }
    }
    usage() if(!defined($host) || !defined($rnum) || $rnum < 1);
    $wnum = DEFWNUM if(!defined($wnum) || $wnum < 1);
    my $rv = procremove($host, $port, $rnum, $wnum, $xb);
    return $rv;
}


# perform write command
sub procwrite {
    my $host = shift;
    my $port = shift;
    my $rnum = shift;
    my $wnum = shift;
    my $xb = shift;
    printf("<Writing Test>\n  host=%s  port=%d  rnum=%d  wnum=%d  xb=%d\n\n",
           $host, $port, $rnum, $wnum, $xb);
    my $err = 0;
    my $stime = gettimeofday();
    my $rdb = TokyoTyrant::RDB->new();
    if(!$rdb->open($host, $port)){
        eprint($rdb, "open");
        $err = 1;
    }
    srand($rnum + $wnum);
    my $vnum = $rnum * VALRATIO;
    $vnum = 1 if($vnum < 1);
    my $seed = rand($vnum);
    for(my $i = 1; $i <= $rnum; $i++){
        my $text = "";
        my $twnum = int(rand($wnum) + 1);
        for(my $j = 0; $j < $twnum; $j++){
            my $rnum = rand($vnum);
            $text .= sprintf("\t%d", int($seed * $rnum / $vnum));
            $seed = $rnum;
        }
        if($xb){
            if(!$rdb->ext("xput", "http://localhost/$i", $text)){
                eprint($rdb, "xext(put)");
                $err = 1;
                last;
            }
        } else {
            if(!$rdb->ext("put", $i, $text)){
                eprint($rdb, "ext(put)");
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
    my $rnum = shift;
    my $pnum = shift;
    my $xb = shift;
    printf("<Reading Test>\n  host=%s  port=%d  rnum=%d  pnum=%d  xb=%d\n\n",
           $host, $port, $rnum, $pnum, $xb);
    my $err = 0;
    my $stime = gettimeofday();
    my $rdb = TokyoTyrant::RDB->new();
    if(!$rdb->open($host, $port)){
        eprint($rdb, "open");
        $err = 1;
    }
    srand($rnum + $pnum);
    my $vnum = $rnum * VALRATIO;
    $vnum = 1 if($vnum < 1);
    my $seed = rand($vnum);
    for(my $i = 1; $i <= $rnum; $i++){
        my $text = "";
        my $tpnum = int(rand($pnum) + 1);
        for(my $j = 0; $j < $tpnum; $j++){
            my $rnum = rand($vnum);
            $text .= sprintf("\t%d", int($seed * $rnum / $vnum));
            $seed = $rnum;
        }
        if($xb){
            if(!defined($rdb->ext("xsearch", $text))){
                eprint($rdb, "ext(xsearch)");
                $err = 1;
                last;
            }
        } else {
            if(!defined($rdb->ext("search", $text))){
                eprint($rdb, "ext(search)");
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
    my $rnum = shift;
    my $wnum = shift;
    my $xb = shift;
    printf("<Removing Test>\n  host=%s  port=%d  rnum=%d  wnum=%d  xb=%d\n\n",
           $host, $port, $rnum, $wnum, $xb);
    my $err = 0;
    my $stime = gettimeofday();
    my $rdb = TokyoTyrant::RDB->new();
    if(!$rdb->open($host, $port)){
        eprint($rdb, "open");
        $err = 1;
    }
    srand($rnum + $wnum);
    my $vnum = $rnum * VALRATIO;
    $vnum = 1 if($vnum < 1);
    my $seed = rand($vnum);
    for(my $i = 1; $i <= $rnum; $i++){
        my $text = "";
        my $twnum = int(rand($wnum) + 1);
        for(my $j = 0; $j < $twnum; $j++){
            my $rnum = rand($vnum);
            $text .= sprintf("\t%d", int($seed * $rnum / $vnum));
            $seed = $rnum;
        }
        if($xb){
            if(!$rdb->ext("xout", "http://localhost/$i")){
                eprint($rdb, "ext(xout)");
                $err = 1;
                last;
            }
        } else {
            if(!$rdb->ext("out", $i, $text)){
                eprint($rdb, "ext(out)");
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


# execute main
$| = 1;
$0 =~ s/.*\///;
exit(main());



# END OF FILE
