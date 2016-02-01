# SortedWordCounter

## Description

This application allows to execute WordCount from a remote file, passing the URL as an execution 
parameter. After counting words, it orders by descending frequency order and store it in a remote FTP 
folder, defined by parameters, as well.

The remote file to be processed, can be just a single plain text file or a zip file with many files
inside, both works the same way.

Libraries like Apache FileUtils, SDK to store/receive data from Amazon S3 and other utilites has been ommited in order to develop with the basis,
the code has been compiled with Java 7 because of the docker containers configuration.

First it was tried to use maven to handle dependencies, but because of problems with some libraries, I 
decided to priorize other developments.  

## Packages structure
1. eu.antoniolopez.mapreduce: contains the main file 
2. eu.antoniolopez.mapreduce.io: contains the classes to work with files: download, unzip, upload,...
3. eu.antoniolopez.mapreduce.mapper: two mappers, one to count words, the other to order by frequency
4. eu.antoniolopez.mapreduce.reducer: two reducers, one to count words, the other to order by frequency


## Installing Docker Containers

Follow these instructions in order to install Docker containers cluster to test this 
MapReducer application: http://kiwenlau.blogspot.com.es/2015/05/quickly-build-arbitrary-size-hadoop.html

This is a 3 node hadoop cluster based on ubuntu, with hadoop 2.3.0 and java 7.

## Start container, hadoop and run command

There is an script in this project root called run.sh, this script does almost the 
same than the original scripts but with some minimal modifications:

1. Doesn't open a bash console with the master container.
2. Copy a root jar file to master container
3. Print the command line to run hadoop with this jar and a sample book: "War and Peace" by Tolstoy: http://www.gutenberg.org/ebooks/2600.txt.utf-8 (3.3MB)

Also has been tested with: https://dumps.wikimedia.org/enwiki/20160113/enwiki-20160113-abstract1.xml 843.6 MB file.  



## Dependencies

All needed dependencies are stored in src/lib folder