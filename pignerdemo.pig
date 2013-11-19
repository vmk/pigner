-- pignerdemo.pig: a small example on how to use the function in piglatin

-- Register 3rd party libraries with pig
register lib/stanford-ner.jar;
register pigner.jar;

-- Define the function
DEFINE pigner nl.surfsara.pig.Pigner();

-- Load some text line by line
lines = LOAD 'sample.txt' USING PigStorage('\n');

-- Apply the function to each line
taggedlines = FOREACH lines GENERATE pigner();

-- Dump output tuples to standard out
dump taggedlines;