#!/bin/bash


PY_FILES=$(find ./ -name '*.py')

for f in $PY_FILES; do  
    if ! grep -q "$(cat copyright.txt)" $f; then
        cp copyright.txt temp.txt
        cat $f >> temp.txt
        mv temp.txt $f
    fi  
done
