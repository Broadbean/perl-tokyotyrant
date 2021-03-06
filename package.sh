#! /bin/sh

LANG=C
LC_ALL=C
PATH="$PATH:/usr/local/bin:$HOME/bin:.:.."
export LANG LC_ALL PATH

if [ -f Makefile ]
then
  make distclean
fi

rm -rf casket casket* *~ *.tmp hoge moge TokyoTyrant-*.tar.gz

name="${PWD##*/}"
cd ..
if [ -d "$name" ]
then
  tar zcvf "$name.tar.gz" "$name"
fi
