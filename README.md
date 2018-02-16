SpatialHadoop
=============

[SpatialHadoop](http://spatialhadoop.cs.umn.edu) is an extension to Hadoop that provides efficient processing of
spatial data using MapReduce. It provides spatial data types to be used in
MapReduce jobs including point, rectangle and polygon. It also adds low level
spatial indexes in HDFS such as Grid file, R-tree and R+-tree. Some new
InputFormats and RecordReaders are also provided to allow reading and processing
spatial indexes efficiently in MapReduce jobs. SpatialHadoop also ships with
a bunch of spatial operations that are implemented as efficient MapReduce jobs
which access spatial indexes using the new components. Developers can implement
myriad spatial operations that run efficiently using spatial indexes.


What is this Fork about?
============
This fork implements the skyline (SAS algorithm) and reverse skyline (SRSAS agorithm) queries. 


Generate and Index Dataset
========

Generate a non-indexed spatial file with points in a rectangular area of 1M x 1M

    shadoop generate test.points size:1.gb shape:point mbr:0,0,1000000,1000000 

Build a rtree index over the generated file

    shadoop index test.points sindex:rtree test.tree shape:point

Run a range query that selects rectangles overlapping the query area defined
by the box with the two corners (10, 20) and (2000, 3000). Results are stored
in the output file *rangequery.out*

    shadoop rangequery test.rtree rect:10,10,2000,3000 rangequery.out shape:rect
    
To make a sampling from an existing dataset in order to create a new one you can use the sampler and sampler2 classes.
    
Tranfer datasets
=======
To copy a dataset from your local disk to HDFS you can use:
 		
 	hadoop fs -copyFromLocal PathToLocalFile PathToHDFSFile

To tranfer in another cluster you can use a command similar to the one bellow:

    hadoop distcp - strategy dynamic -overwrite webhdfs://clusterIP:ClusterPort/PathToFolder
    
