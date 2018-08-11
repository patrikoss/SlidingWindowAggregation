# Sliding aggregation

This is a spark program implementing sliding aggregation algorithm from 
http://www.cse.cuhk.edu.hk/~taoyf/paper/sigmod13-mr.pdf, section: 5.2.

## Prerequisites
- python 3.5
- pyspark 2.3

## Starting the program
- Set up pyspark on your cluster of computers
- spark submit [spark_options] sliding_aggregation.py path_to_input_on_hdfs window_size path_to_output_on_hdfs number_of_partitions

## Files:
- sliding_aggregation.py - source code
- tests.py - tests

## Input format:
Collection of files of the form:
number weight

e.g.
file1.txt
1 2
2 5
5 8

file2.txt
2 3
5 6
9 1
