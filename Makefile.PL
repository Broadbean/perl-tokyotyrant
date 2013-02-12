#=================================================================================================
# Script for MakeMaker
#=================================================================================================


use ExtUtils::MakeMaker;
use strict;
use warnings;


# Create Makefile
WriteMakefile(
    'NAME'              => 'TokyoTyrant',
    'VERSION_FROM'      => 'TokyoTyrant.pm',
    'clean'             => { FILES => "casket* *~ hoge moge" },
);



# END OF FILE
