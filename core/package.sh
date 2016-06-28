#!/bin/bash

WD=target/

MODULE=spark-tk

pushd $WD

	mkdir -p $MODULE/dependencies
	
	cp core*.jar $MODULE/
	
	cp -Rv dependencies/* $MODULE/dependencies/
	
	pushd $MODULE
	ln -s dependencies lib
	popd

	zip --symlinks -r $MODULE.zip $MODULE

	rm -rf $MODULE

popd
