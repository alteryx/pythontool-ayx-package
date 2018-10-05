#!/bin/bash


PY_FILES=$(find ./ -name '*.py')
for f in $PY_FILES; do 
    cp copyright.txt temp.txt
    cat $f >> temp.txt
    mv temp.txt $f
done
