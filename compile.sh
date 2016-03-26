#! /bin/bash

target="test.bin"
targetDir="/Volumes/DATA/Documents/2016/Data/"

run_clean()
{
    # echo ${target}.*
    (cd ${targetDir} && rm -rf ${target} ${target}.*)
}

if [ "$1" == "clean" ]; then
    run_clean
    exit 0
fi

make -f Makefile.MacOS

if [ "$?" == "0" ]; then
    mv ${target} ${target}.* $targetDir
else
    echo "compile error!"
fi
