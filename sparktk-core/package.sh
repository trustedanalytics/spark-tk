#!/bin/bash

WD=target/

MODULE=sparktk-core

pushd $WD

	mkdir -p $MODULE/dependencies
	
	cp sparktk-core*.jar $MODULE/

    for source in `find \`pwd\` -iname "*sources.jar"`
    do
    if [ "$source" != "" ]; then
    echo remove source file $source
	rm  $source
	fi
	done

	cp -Rv dependencies/* $MODULE/dependencies/
	
	pushd $MODULE
	ln -s dependencies lib
	popd

	zip --symlinks -r $MODULE.zip $MODULE

	rm -rf $MODULE

popd
