#!/bin/bash

mkdir bin
mkdir release

javac src/br/ufrj/ppgi/huffmanmapreduce/*.java src/br/ufrj/ppgi/huffmanmapreduce/mapreduce/encoder/*.java src/br/ufrj/ppgi/huffmanmapreduce/mapreduce/io/*.java src/br/ufrj/ppgi/huffmanmapreduce/mapreduce/symbolcount/*.java -d bin

ant
