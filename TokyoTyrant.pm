#-------------------------------------------------------------------------------------------------
# Pure Perl interface of Tokyo Tyrant
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


package TokyoTyrant;

use strict;
use warnings;
use bytes;
use Carp;

require Exporter;
use base qw(Exporter);
our $VERSION = '1.16';
our $DEBUG = 0;



#----------------------------------------------------------------
# utilities
#----------------------------------------------------------------


sub atoi {
    my $str = shift;
    return 0 if(!defined($str));
    $str =~ s/^ *//;
    my $sign = 1;
    if($str =~ /^-/){
        $sign = -1;
        $str =~ s/^-*//
    }
    return 0 unless $str =~ /^\d/;
    $str =~ s/[^\d].*//g;
    return int($str) * $sign;
}


sub atof {
    my $str = shift;
    return 0 if(!defined($str));
    my $epow = 1;
    $str = sprintf("%f", $str) if($str =~ /^ *\d+(.\d+)*e\+\d/);
    $str =~ s/^ *//;
    my $sign = 1;
    if($str =~ /^-/){
        $sign = -1;
        $str =~ s/^-*//
    }
    return 0 unless $str =~ /^\d/;
    $str =~ s/[^\d.].*//g;
    return $str * $epow * $sign;
}



#----------------------------------------------------------------
# the remote database API
#----------------------------------------------------------------


package TokyoTyrant::RDB;

use strict;
use warnings;
use bytes;
use Carp;
use Encode;
use Socket qw(:all);
use POSIX;

use constant {
    ESUCCESS => 0,
    EINVALID => 1,
    ENOHOST => 2,
    EREFUSED => 3,
    ESEND => 4,
    ERECV => 5,
    EKEEP => 6,
    ENOREC => 7,
    EMISC => 9999,
};

use constant {
    XOLCKREC => 1 << 0,
    XOLCKGLB => 1 << 1,
};

use constant {
    MONOULOG => 1 << 0,
};

use constant {
    _BIGENDIAN => unpack("L", "\x00\x00\x00\x01") == 1,
    _QUADBASE => 2 ** 32,
};


sub new {
    my $class = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $self = {
        _ecode => ESUCCESS,
        _sock => undef,
        _tout => undef,
    };
    bless($self, $class);
    return $self;
}


sub DESTROY {
    my $self = shift;
    $self->close() if(defined($self->{_sock}));
    return undef;
}


sub errmsg {
    my $self = shift;
    my $ecode = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    $ecode = $self->{_ecode} if(!defined($ecode));
    if($ecode == ESUCCESS){
        return "success";
    } elsif($ecode == EINVALID){
        return "invalid operation";
    } elsif($ecode == ENOHOST){
        return "host not found";
    } elsif($ecode == EREFUSED){
        return "connection refused";
    } elsif($ecode == ESEND){
        return "send error";
    } elsif($ecode == ERECV){
        return "recv error";
    } elsif($ecode == EKEEP){
        return "existing record";
    } elsif($ecode == ENOREC){
        return "no record found";
    } elsif($ecode == EMISC){
        return "miscellaneous error";
    }
    return "unknown";
}


sub ecode {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    return $self->{_ecode};
}


sub open {
    my $self = shift;
    my $host = shift;
    my $port = shift;
    my $timeout = shift;
    if(scalar(@_) != 0 || !defined($host) || length($host) < 1){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    if(defined($port) && $port > 0){
        my $addr = inet_aton($host);
        if(!defined($addr)){
            $self->{_ecode} = ENOHOST;
            return 0;
        }
        my $sin = sockaddr_in($port, $addr);
        if(!socket($sock, PF_INET, SOCK_STREAM, getprotobyname('tcp'))){
            $self->{_ecode} = EREFUSED;
            return 0;
        }
        setsockopt($sock, IPPROTO_TCP, TCP_NODELAY, 1);
        if(!connect($sock, $sin)){
            $self->{_ecode} = EREFUSED;
            close($sock);
            return 0;
        }
    } else {
        my $sun = sockaddr_un($host);
        if(!socket($sock, PF_UNIX, SOCK_STREAM, 0)){
            $self->{_ecode} = EREFUSED;
            return 0;
        }
        if(!connect($sock, $sun)){
            $self->{_ecode} = EREFUSED;
            close($sock);
            return 0;
        }
    }
    $self->{_sock} = $sock;
    $self->{_tout} = defined($timeout) && $timeout > 0 ? $timeout : 0;
    return 1;
}


sub close {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    if(!close($self->{_sock})){
        $self->{_ecode} = EMISC;
        $self->{_sock} = undef;
        return 0;
    }
    $self->{_sock} = undef;
    return 1;
}


sub put {
    my $self = shift;
    my $key = shift;
    my $value = shift;
    if(scalar(@_) != 0 || !defined($key) || !defined($value)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CCNN", 0xC8, 0x10, bytes::length($key), bytes::length($value));
    $sbuf .= $key . $value;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    return 1;
}


sub putkeep {
    my $self = shift;
    my $key = shift;
    my $value = shift;
    if(scalar(@_) != 0 || !defined($key) || !defined($value)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CCNN", 0xC8, 0x11, bytes::length($key), bytes::length($value));
    $sbuf .= $key . $value;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EKEEP;
        return 0;
    }
    return 1;
}


sub putcat {
    my $self = shift;
    my $key = shift;
    my $value = shift;
    if(scalar(@_) != 0 || !defined($key) || !defined($value)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CCNN", 0xC8, 0x12, bytes::length($key), bytes::length($value));
    $sbuf .= $key . $value;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    return 1;
}


sub putshl {
    my $self = shift;
    my $key = shift;
    my $value = shift;
    my $width = shift;
    if(scalar(@_) != 0 || !defined($key) || !defined($value) || !defined($width)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    $width = TokyoTyrant::atoi($width);
    $width = 0 if($width < 0);
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CCNNN", 0xC8, 0x13, bytes::length($key), bytes::length($value), $width);
    $sbuf .= $key . $value;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    return 1;
}


sub putnr {
    my $self = shift;
    my $key = shift;
    my $value = shift;
    if(scalar(@_) != 0 || !defined($key) || !defined($value)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CCNN", 0xC8, 0x18, bytes::length($key), bytes::length($value));
    $sbuf .= $key . $value;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    return 1;
}


sub out {
    my $self = shift;
    my $key = shift;
    if(scalar(@_) != 0 || !defined($key)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CCN", 0xC8, 0x20, bytes::length($key));
    $sbuf .= $key;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = ENOREC;
        return 0;
    }
    return 1;
}


sub get {
    my $self = shift;
    my $key = shift;
    if(scalar(@_) != 0 || !defined($key)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return undef;
    }
    my $sbuf = pack("CCN", 0xC8, 0x30, bytes::length($key));
    $sbuf .= $key;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return undef;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return undef;
    }
    if($code != 0){
        $self->{_ecode} = ENOREC;
        return undef;
    }
    my $vsiz = $self->_recvint32();
    if($vsiz < 0){
        $self->{_ecode} = ERECV;
        return undef;
    }
    my $vref = $self->_recv($vsiz);
    if(!defined($vref)){
        $self->{_ecode} = ERECV;
        return undef;
    }
    return $$vref;
}


sub mget {
    my $self = shift;
    my $recs = shift;
    if(scalar(@_) != 0 || !defined($recs) || ref($recs) ne 'HASH'){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return -1;
    }
    my $rnum = 0;
    my $sbuf = "";
    while(my ($key, $value) = each(%$recs)){
        $sbuf .= pack("N", bytes::length($key)) . $key;
        $rnum++;
    }
    $sbuf = pack("CCN", 0xC8, 0x31, $rnum) . $sbuf;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return -1;
    }
    my $code = $self->_recvcode();
    $rnum = $self->_recvint32();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return -1;
    }
    if($code != 0){
        $self->{_ecode} = ENOREC;
        return -1;
    }
    if($rnum < 0){
        $self->{_ecode} = ERECV;
        return -1;
    }
    %$recs = ();
    for(my $i = 0; $i < $rnum; $i++){
        my $ksiz = $self->_recvint32();
        my $vsiz = $self->_recvint32();
        if($ksiz < 0 || $vsiz < 0){
            $self->{_ecode} = ERECV;
            return -1;
        }
        my $kref = $self->_recv($ksiz);
        my $vref = $self->_recv($vsiz);
        if(!defined($kref) || !defined($vref)){
            $self->{_ecode} = ERECV;
            return -1;
        }
        $recs->{$$kref} = $$vref;
    }
    return $rnum;
}


sub vsiz {
    my $self = shift;
    my $key = shift;
    if(scalar(@_) != 0 || !defined($key)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return -1;
    }
    my $sbuf = pack("CCN", 0xC8, 0x38, bytes::length($key));
    $sbuf .= $key;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return -1;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return -1;
    }
    if($code != 0){
        $self->{_ecode} = ENOREC;
        return -1;
    }
    return $self->_recvint32();
}


sub iterinit {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CC", 0xC8, 0x50);
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    return 1;
}


sub iternext {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return undef;
    }
    my $sbuf = pack("CC", 0xC8, 0x51);
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return undef;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return undef;
    }
    if($code != 0){
        $self->{_ecode} = ENOREC;
        return undef;
    }
    my $ksiz = $self->_recvint32();
    if($ksiz < 0){
        $self->{_ecode} = ERECV;
        return undef;
    }
    my $kref = $self->_recv($ksiz);
    if(!defined($kref)){
        $self->{_ecode} = ERECV;
        return undef;
    }
    return $$kref;
}


sub fwmkeys {
    my $self = shift;
    my $prefix = shift;
    my $max = shift;
    if(scalar(@_) != 0 || !defined($prefix)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my @keys = ();
    $max = 1 << 31 if(!defined($max));
    $max = TokyoTyrant::atoi($max);
    $max = 1 << 31 if($max < 0);
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return \@keys;
    }
    my $sbuf = pack("CCNN", 0xC8, 0x58, bytes::length($prefix), $max);
    $sbuf .= $prefix;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return \@keys;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return \@keys;
    }
    if($code != 0){
        $self->{_ecode} = ENOREC;
        return \@keys;
    }
    my $knum = $self->_recvint32();
    if($knum < 0){
        $self->{_ecode} = ERECV;
        return \@keys;
    }
    for(my $i = 0; $i < $knum; $i++){
        my $ksiz = $self->_recvint32();
        if($ksiz < 0){
            $self->{_ecode} = ERECV;
            return \@keys;
        }
        my $kref = $self->_recv($ksiz);
        if(!defined($kref)){
            $self->{_ecode} = ERECV;
            return \@keys;
        }
        push(@keys, $$kref);
    }
    return \@keys;
}


sub addint {
    my $self = shift;
    my $key = shift;
    my $num = shift;
    if(scalar(@_) != 0 || !defined($key)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    $num = TokyoTyrant::atoi($num);
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return undef;
    }
    my $sbuf = pack("CCNN", 0xC8, 0x60, bytes::length($key), $num);
    $sbuf .= $key;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return undef;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return undef;
    }
    if($code != 0){
        $self->{_ecode} = EKEEP;
        return undef;
    }
    return $self->_recvint32();
}


sub adddouble {
    my $self = shift;
    my $key = shift;
    my $num = shift;
    if(scalar(@_) != 0 || !defined($key)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    $num = TokyoTyrant::atof($num);
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return undef;
    }
    my ($fract, $integ) = POSIX::modf($num);
    $fract = int($fract * 1000000000000);
    my $sbuf = pack("CCN", 0xC8, 0x61, bytes::length($key));
    $sbuf .= _packquad($integ) . _packquad($fract);
    $sbuf .= $key;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return undef;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return undef;
    }
    if($code != 0){
        $self->{_ecode} = EKEEP;
        return undef;
    }
    $integ = $self->_recvint64();
    $fract = $self->_recvint64();
    return $integ + $fract / 1000000000000;
}


sub ext {
    my $self = shift;
    my $name = shift;
    my $key = shift;
    my $value = shift;
    my $xopts = shift;
    if(scalar(@_) != 0 || !defined($name)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    $key = "" if(!defined($key));
    $value = "" if(!defined($value));
    $xopts = TokyoTyrant::atoi($xopts);
    $xopts = 0 if($xopts < 0);
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return undef;
    }
    my $sbuf = pack("CCNNNN", 0xC8, 0x68,
                    bytes::length($name), $xopts, bytes::length($key), bytes::length($value));
    $sbuf .= $name . $key . $value;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return undef;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return undef;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return undef;
    }
    my $xsiz = $self->_recvint32();
    if($xsiz < 0){
        $self->{_ecode} = ERECV;
        return undef;
    }
    my $xref = $self->_recv($xsiz);
    if(!defined($xref)){
        $self->{_ecode} = ERECV;
        return undef;
    }
    return $$xref;
}


sub sync {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CC", 0xC8, 0x70);
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    return 1;
}


sub optimize {
    my $self = shift;
    my $params = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    $params = "" if(!defined($params));
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CCN", 0xC8, 0x71, bytes::length($params));
    $sbuf .= $params;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    return 1;
}


sub vanish {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CC", 0xC8, 0x72);
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    return 1;
}


sub copy {
    my $self = shift;
    my $path = shift;
    if(scalar(@_) != 0 || !defined($path)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CCN", 0xC8, 0x73, bytes::length($path));
    $sbuf .= $path;
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    return 1;
}


sub rnum {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CC", 0xC8, 0x80);
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    my $rv = $self->_recvint64();
    if($rv < 0){
        $self->{_ecode} = ERECV;
        return 0;
    }
    return $rv;
}


sub size {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return 0;
    }
    my $sbuf = pack("CC", 0xC8, 0x81);
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return 0;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return 0;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return 0;
    }
    my $rv = $self->_recvint64();
    if($rv < 0){
        $self->{_ecode} = ERECV;
        return 0;
    }
    return $rv;
}


sub stat {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return undef;
    }
    my $sbuf = pack("CC", 0xC8, 0x88);
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return undef;
    }
    my $code = $self->_recvcode();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return undef;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return undef;
    }
    my $ssiz = $self->_recvint32();
    if($ssiz < 0){
        $self->{_ecode} = ERECV;
        return undef;
    }
    my $sref = $self->_recv($ssiz);
    if(!defined($sref)){
        $self->{_ecode} = ERECV;
        return undef;
    }
    return $$sref;
}


sub misc {
    my $self = shift;
    my $name = shift;
    my $args = shift;
    my $mopts = shift;
    if(scalar(@_) != 0 || !defined($name)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    $args = [] if(!defined($args) || ref($args) ne 'ARRAY');
    $mopts = TokyoTyrant::atoi($mopts);
    $mopts = 0 if($mopts < 0);
    my $sock = $self->{_sock};
    if(!defined($sock)){
        $self->{_ecode} = EINVALID;
        return undef;
    }
    my $sbuf = pack("CCNNN", 0xC8, 0x90, bytes::length($name), $mopts, scalar(@$args));
    $sbuf .= $name;
    foreach my $arg (@$args){
        $sbuf .= pack("N", bytes::length($arg)) . $arg;
    }
    if(!$self->_send(\$sbuf)){
        $self->{_ecode} = ESEND;
        return undef;
    }
    my $code = $self->_recvcode();
    my $rnum = $self->_recvint32();
    if($code == -1){
        $self->{_ecode} = ERECV;
        return undef;
    }
    if($code != 0){
        $self->{_ecode} = EMISC;
        return undef;
    }
    my @res;
    for(my $i = 0; $i < $rnum; $i++){
        my $esiz = $self->_recvint32();
        if($esiz < 0){
            $self->{_ecode} = ERECV;
            return undef;
        }
        my $eref = $self->_recv($esiz);
        if(!defined($eref)){
            $self->{_ecode} = ERECV;
            return undef;
        }
        push(@res, $$eref);
    }
    return \@res;
}


sub TIEHASH {
    my $class = shift;
    my $host = shift;
    my $port = shift;
    my $rdb = $class->new();
    return undef if(!$rdb->open($host, $port));
    return $rdb;
}


sub UNTIE {
    my $self = shift;
    return $self->close();
}


sub STORE {
    my $self = shift;
    my $key = shift;
    my $value = shift;
    return $self->put($key, $value);
}


sub DELETE {
    my $self = shift;
    my $key = shift;
    return $self->out($key);
}


sub FETCH {
    my $self = shift;
    my $key = shift;
    return $self->get($key);
}


sub EXISTS {
    my $self = shift;
    my $key = shift;
    return $self->vsiz($key) >= 0;
}


sub FIRSTKEY {
    my $self = shift;
    $self->iterinit();
    return $self->iternext();
}


sub NEXTKEY {
    my $self = shift;
    return $self->iternext();
}


sub CLEAR {
    my $self = shift;
    return $self->vanish();
}


sub _send {
    my $self = shift;
    my $ref = shift;
    my $len = bytes::length($$ref);
    while(1){
        if($self->{_tout} > 0){
            my $bits = "";
            vec($bits, fileno($self->{_sock}), 1) = 1;
            return 0 if(select(undef, $bits, undef, $self->{_tout}) < 1);
        }
        my $rv = send($self->{_sock}, $$ref, 0);
        return 0 if(!defined($rv));
        last if($rv == $len);
        my $str = substr($$ref, $rv);
        $len = bytes::length($str);
        $ref = \$str;
    }
    return 1;
}


sub _recv {
    my $self = shift;
    my $len = shift;
    my $str = "";
    return \$str if($len < 1);
    if($self->{_tout} > 0){
        my $bits = "";
        vec($bits, fileno($self->{_sock}), 1) = 1;
        return undef if(select($bits, undef, undef, $self->{_tout}) < 1);
    }
    return undef if(!defined(recv($self->{_sock}, $str, $len, 0)));
    return \$str if(bytes::length($str) == $len);
    $len -= bytes::length($str);
    while($len > 0){
        my $tstr = "";
        if($self->{_tout} > 0){
            my $bits = "";
            vec($bits, fileno($self->{_sock}), 1) = 1;
            return undef if(select($bits, undef, undef, $self->{_tout}) < 1);
        }
        return undef if(!defined(recv($self->{_sock}, $tstr, $len, 0)));
        if(bytes::length($tstr) < 1){
            return undef if(!defined(recv($self->{_sock}, $tstr, $len, 0)));
            return undef if(bytes::length($tstr) < 1);
        }
        $len -= bytes::length($tstr);
        $str .= $tstr;
    }
    return \$str;
}


sub _recvcode {
    my $self = shift;
    my $rbuf = $self->_recv(1);
    return -1 if(!defined($rbuf));
    return unpack("C", $$rbuf);
}


sub _recvint32 {
    my $self = shift;
    my $rbuf = $self->_recv(4);
    return 0 if(!defined($rbuf));
    my $num = unpack("N", $$rbuf);
    return unpack("l", pack("l", $num));
}


sub _recvint64 {
    my $self = shift;
    my $rbuf = $self->_recv(8);
    return -1 if(!defined($rbuf));
    my ($high, $low) = unpack("NN", $$rbuf);
    my $num = $high * _QUADBASE + $low;
    eval {
        $num = unpack("q", pack("q", $num));
    };
    return $num;
}


sub _hton {
    return shift if(_BIGENDIAN);
    my @chars = split('', shift);
    my $str = "";
    foreach my $char (@chars){
        $str = $char . $str;
    }
    return $str;
}


sub _ntoh {
    return shift if(_BIGENDIAN);
    my @chars = split('', shift);
    my $str = "";
    foreach my $char (@chars){
        $str = $char . $str;
    }
    return $str;
}


sub _packquad {
    my $num = shift;
    my $high = int($num / _QUADBASE);
    my $low = $num % _QUADBASE;
    return pack("NN", $high, $low);
}



#----------------------------------------------------------------
# the table extension of the remote database API
#----------------------------------------------------------------


package TokyoTyrant::RDBTBL;

use strict;
use warnings;
use bytes;
use Carp;
use Encode;
use Socket;
use POSIX;

use base qw(TokyoTyrant::RDB);

use constant {
  ITLEXICAL => 0,
  ITDECIMAL => 1,
  ITTOKEN => 2,
  ITQGRAM => 3,
  ITOPT => 9998,
  ITVOID => 9999,
  ITKEEP => 1 << 24,
};


sub put {
    my $self = shift;
    my $pkey = shift;
    my $cols = shift;
    if(scalar(@_) != 0 || !defined($pkey) || !defined($cols) || ref($cols) ne "HASH"){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my @args;
    push(@args, $pkey);
    while(my ($ckey, $cvalue) = each(%$cols)){
        push(@args, $ckey);
        push(@args, $cvalue);
    }
    my $rv = $self->misc("put", \@args, 0);
    return defined($rv) ? 1 : 0;
}


sub putkeep {
    my $self = shift;
    my $pkey = shift;
    my $cols = shift;
    if(scalar(@_) != 0 || !defined($pkey) || !defined($cols) || ref($cols) ne "HASH"){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my @args;
    push(@args, $pkey);
    while(my ($ckey, $cvalue) = each(%$cols)){
        push(@args, $ckey);
        push(@args, $cvalue);
    }
    my $rv = $self->misc("putkeep", \@args, 0);
    if(!defined($rv)){
        $self->{_ecode} = $self->EKEEP if($self->{_ecode} == $self->EMISC);
        return 0;
    }
    return 1;
}


sub putcat {
    my $self = shift;
    my $pkey = shift;
    my $cols = shift;
    if(scalar(@_) != 0 || !defined($pkey) || !defined($cols) || ref($cols) ne "HASH"){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my @args;
    push(@args, $pkey);
    while(my ($ckey, $cvalue) = each(%$cols)){
        push(@args, $ckey);
        push(@args, $cvalue);
    }
    my $rv = $self->misc("putcat", \@args, 0);
    return defined($rv) ? 1 : 0;
}


sub out {
    my $self = shift;
    my $pkey = shift;
    if(scalar(@_) != 0 || !defined($pkey)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my @args;
    push(@args, $pkey);
    my $rv = $self->misc("out", \@args, 0);
    if(!defined($rv)){
        $self->{_ecode} = $self->ENOREC if($self->{_ecode} == $self->EMISC);
        return 0;
    }
    return 1;
}


sub get {
    my $self = shift;
    my $pkey = shift;
    if(scalar(@_) != 0 || !defined($pkey)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my @args;
    push(@args, $pkey);
    my $rv = $self->misc("get", \@args, $self->MONOULOG);
    if(!defined($rv)){
        $self->{_ecode} = $self->ENOREC if($self->{_ecode} == $self->EMISC);
        return undef;
    }
    my %cols = @$rv;
    return \%cols;
}


sub mget {
    my $self = shift;
    my $recs = shift;
    if(scalar(@_) != 0 || !defined($recs) || ref($recs) ne 'HASH'){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $rv = $self->SUPER::mget($recs);
    return -1 if($rv < 0);
    while(my ($pkey, $value) = each(%$recs)){
        my %cols = split(/\0/ , $value);
        $$recs{$pkey} = \%cols;
    }
    return $rv;
}


sub setindex {
    my $self = shift;
    my $name = shift;
    my $type = shift;
    if(scalar(@_) != 0 || !defined($name) || !defined($type)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my @args;
    push(@args, $name);
    push(@args, $type);
    my $rv = $self->misc("setindex", \@args, 0);
    return defined($rv) ? 1 : 0;
}


sub genuid {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $rv = $self->misc("genuid", [], 0);
    return -1 if(!defined($rv));
    return $$rv[0];
}



package TokyoTyrant::RDBQRY;

use strict;
use warnings;
use bytes;
use Carp;
use Encode;

use constant {
  QCSTREQ => 0,
  QCSTRINC => 1,
  QCSTRBW => 2,
  QCSTREW => 3,
  QCSTRAND => 4,
  QCSTROR => 5,
  QCSTROREQ => 6,
  QCSTRRX => 7,
  QCNUMEQ => 8,
  QCNUMGT => 9,
  QCNUMGE => 10,
  QCNUMLT => 11,
  QCNUMLE => 12,
  QCNUMBT => 13,
  QCNUMOREQ => 14,
  QCFTSPH => 15,
  QCFTSAND => 16,
  QCFTSOR => 17,
  QCFTSEX => 18,
  QCNEGATE => 1 << 24,
  QCNOIDX => 1 << 25,
};

use constant {
  QOSTRASC => 0,
  QOSTRDESC => 1,
  QONUMASC => 2,
  QONUMDESC => 3,
};

use constant {
  MSUNION => 0,
  MSISECT => 1,
  MSDIFF => 2,
};


sub new {
    my $class = shift;
    my $rdb = shift;
    if(scalar(@_) != 0 || !defined($rdb) || ref($rdb) ne "TokyoTyrant::RDBTBL"){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $self = [0, 0];
    $$self[0] = $rdb;
    $$self[1] = [ "hint" ];
    $$self[2] = "";
    bless($self, $class);
    return $self;
}


sub addcond {
    my $self = shift;
    my $name = shift;
    my $op = shift;
    my $expr = shift;
    if(scalar(@_) != 0 || !defined($name) || !defined($op) || !defined($expr)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $args = $$self[1];
    my $buf = "addcond" . "\0" . $name . "\0" . $op . "\0" . $expr;
    push(@$args, $buf);
    return undef;
}


sub setorder {
    my $self = shift;
    my $name = shift;
    my $type = shift;
    if(scalar(@_) != 0 || !defined($name)){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    $type = $self->QOSTRASC if(!defined($type));
    my $args = $$self[1];
    my $buf = "setorder" . "\0" . $name . "\0" . $type;
    push(@$args, $buf);
    return undef;
}


sub setlimit {
    my $self = shift;
    my $max = shift;
    my $skip = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    $max = -1 if(!defined($max));
    $skip = -1 if(!defined($skip));
    my $args = $$self[1];
    my $buf = "setlimit" . "\0" . $max . "\0" . $skip;
    push(@$args, $buf);
    return undef;
}


sub search {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $rdb = $$self[0];
    my $args = $$self[1];
    $$self[2] = "";
    my $rv = $rdb->misc("search", $args, $rdb->MONOULOG);
    return [] if(!defined($rv));
    $self->popmeta($rv);
    return $rv;
}


sub searchout {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $rdb = $$self[0];
    my $oargs = $$self[1];
    my @args = @$oargs;
    push(@args, "out");
    $$self[2] = "";
    my $rv = $rdb->misc("search", \@args, 0);
    return 0 if(!defined($rv));
    $self->popmeta($rv);
    return 1;
}


sub searchget {
    my $self = shift;
    my $names = shift;
    if(scalar(@_) != 0 || (defined($names) && ref($names) ne 'ARRAY')){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $rdb = $$self[0];
    my $oargs = $$self[1];
    my @args = @$oargs;
    if(defined($names)){
        push(@args, "get" . "\0" . join("\0", @$names));
    } else {
        push(@args, "get");
    }
    $$self[2] = "";
    my $rv = $rdb->misc("search", \@args, $rdb->MONOULOG);
    return [] if(!defined($rv));
    $self->popmeta($rv);
    for(my $i = 0; $i < scalar(@$rv); $i++){
        my %cols = split(/\0/ , $$rv[$i]);
        $$rv[$i] = \%cols;
    }
    return $rv;
}


sub searchcount {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($TokyoTyrant::DEBUG);
        return undef;
    }
    my $rdb = $$self[0];
    my $oargs = $$self[1];
    my @args = @$oargs;
    push(@args, "count");
    $$self[2] = "";
    my $rv = $rdb->misc("search", \@args, $rdb->MONOULOG);
    return 0 if(!defined($rv));
    $self->popmeta($rv);
    return scalar(@$rv) > 0 ? int($$rv[0]) : 0;
}


sub hint {
    my $self = shift;
    if(scalar(@_) != 0){
        croak((caller(0))[3] . ": invalid parameter") if($Tokyotyrant::DEBUG);
        return undef;
    }
    return $$self[2];
}


sub metasearch {
    my $self = shift;
    my $others = shift;
    my $type = shift;
    if(scalar(@_) != 0 || !defined($others) || ref($others) ne "ARRAY"){
        croak((caller(0))[3] . ": invalid parameter") if($Tokyotyrant::DEBUG);
        return undef;
    }
    $type = $self->MSUNION if(!defined($type));
    my $rdb = $$self[0];
    my $oargs = $$self[1];
    my @args = @$oargs;
    foreach my $other (@$others){
        next if(ref($other) ne "TokyoTyrant::RDBQRY");
        push(@args, "next");
        my $targs = $$other[1];
        foreach my $targ (@$targs){
            push(@args, $targ);
        }
    }
    push(@args, "mstype\0" . $type);
    $$self[2] = "";
    my $rv = $rdb->misc("metasearch", \@args, $rdb->MONOULOG);
    return [] if(!defined($rv));
    $self->popmeta($rv);
    return $rv;
}


sub popmeta {
    my $qry = shift;
    my $res = shift;
    for(my $i = scalar(@$res) - 1; $i >= 0; $i--){
        my $pkey = $$res[$i];
        if($pkey =~ /^\0\0\[\[HINT\]\]\n/){
            $pkey =~ s/^\0\0\[\[HINT\]\]\n//;
            $$qry[2] = $pkey;
            pop(@$res);
        } else {
            last;
        }
    }
}



1;


# END OF FILE
