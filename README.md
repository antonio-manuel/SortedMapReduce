# SortedWordCounter

## Description

This application allows to execute WordCount from a remote file, passing the URL as an execution 
parameter. After counting words, it orders by descending frequency order and store it in a remote 
folder, defined by parameters, as well.

The remote file to be processed, can be just a single plain text file, or a zip file with many files
inside, both works the same way.


## Packages structure
1. eu.antoniolopez.mapreduce: contains the main file 
2. eu.antoniolopez.mapreduce.io: contains the classes to work with files: download, unzip, upload,...
3. eu.antoniolopez.mapreduce.mapper: two mappers, one to count words, the other to order by frequency
4. eu.antoniolopez.mapreduce.reducer: two reducers, one to count words, the other to order by frequency


## Installing Docker Containers

Follow these instructions in order to install Docker containers cluster to test this 
MapReducer application: http://kiwenlau.blogspot.com.es/2015/05/quickly-build-arbitrary-size-hadoop.html

## Run the container and the tast all in one

There is an script in this project root called run.sh, this script does almost the 
same than the original scripts but with some minimal modifications:

1. Doesn't open a bash console with the master container.
2. Copy a root jar file to master container
3. Run hadoop with this jar and a sample book: "War and Peace" by Tolstoy.


## Dependencies

All needed dependencies are stored in src/lib folder
