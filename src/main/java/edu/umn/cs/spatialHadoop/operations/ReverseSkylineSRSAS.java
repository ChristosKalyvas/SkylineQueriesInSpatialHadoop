/***********************************************************************
@author Christos Kalyvas, Manolis Maragoudakis
*************************************************************************/
package edu.umn.cs.spatialHadoop.operations;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.OperationsParams.Direction;
import edu.umn.cs.spatialHadoop.core.GridRecordWriter;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.mapred.BlockFilter;
import edu.umn.cs.spatialHadoop.mapred.DefaultBlockFilter;
import edu.umn.cs.spatialHadoop.mapred.ShapeInputFormat;
import edu.umn.cs.spatialHadoop.mapred.ShapeIterInputFormat;
import edu.umn.cs.spatialHadoop.mapred.SpatialRecordReader.ShapeIterator;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.spatialHadoop.operations.RangeQuery.RangeQueryMap;
import edu.umn.cs.spatialHadoop.util.MemoryReporter;
import edu.umn.cs.spatialHadoop.util.Parallel;
import edu.umn.cs.spatialHadoop.util.ResultCollectorSynchronizer;
import edu.umn.cs.spatialHadoop.util.Parallel.RunnableRange;

/**
 * Computes the reverse skyline of a set of points
 *
 */
public class ReverseSkylineSRSAS {

  private static final Log LOG = LogFactory.getLog(ReverseSkylineSRSAS.class);

  private static int onTheAxisPoints=0;
   


  /**
   * Returns the quadrant that a point belongs based on query point. 
   * 1:MinMin , 2:MaxMin , 3:MinMax , 4:MaxMax.
   * @param p1
   * @param p2
   * @param dir
   * @return
   */
  
  public static int quadrant(Point p, Point q) {
	  int quadrant=-1;
	  double x= p.x-q.x;
	  double y= p.y-q.y; 

	  if(x>0 && y>0){
		  quadrant=4;  //"MaxMax";
	  }else if(x>0 && y<0){
		  quadrant=2;  //"MaxMin";
	  }else if(x<0 && y>0){
		  quadrant=3;  //"MinMax";
	  }else if(x<0 && y<0){
		  quadrant=1;   //"MinMin";
	  }else if(x==0 && y==0){  
		  LOG.info("\t\tCall: We have a query point match in <quadrant(Point p, Point q)> ."); 
	  }else if(x==0 || y==0){ 
		  onTheAxisPoints++;
		  LOG.info("\t\tCall: We have an on the axis point. Found so far: "+onTheAxisPoints);
	  }else{	
		  throw new RuntimeException("Unknown quadrant for Point "+p.toString()+" in <quadrant(Point p, Point q)>.");
	  }
	  return quadrant;
  }
  
  
  /**
   * Returns true if p1 dominates p2 according to the given direction.
   * @param p1
   * @param p2
   * @param dir
   * @return
   */
  private static boolean skylineReverseDominate(Point p1, Point p2, Point q) {    
	  boolean sameguadrant = ((p1.x-q.x)*(p2.x-q.x)>=0) && ((p1.x-q.x)*(p2.x-q.x)>=0);
	  boolean dominated = ( Math.abs(p1.x-q.x) <= Math.abs(p2.x-q.x) && Math.abs(p1.y-q.y) <= Math.abs(p2.y-q.y) ); //Better at least in one dimension
	  return sameguadrant&&dominated;
    } 
  

  public static Point[] ReverseskylineInMemory(Point[] pointsarray, Direction dir, OperationsParams params) throws  IOException, InterruptedException {
	
	Point queryPoint = params.getQueryPoint("query", new Point(0,0));

	double distance = 0; boolean dominance=false;
	   	  
     Rectangle axesXLine = new Rectangle();
     Rectangle axesYLine = new Rectangle(); 
     
     axesXLine.x1 = Double.MIN_VALUE; axesXLine.y1 = queryPoint.y; axesXLine.x2 = Double.MAX_VALUE;  axesXLine.y2 = queryPoint.y;
     axesYLine.x1 = queryPoint.x; axesYLine.y1 = Double.MIN_VALUE; axesYLine.x2 = queryPoint.x; axesYLine.y2 = Double.MAX_VALUE;
     
	Vector<Point> mapList = new Vector<Point>();
	Vector<Point> globalskyList = new Vector<Point>();
	Vector<Rectangle> RangeRectangles = new Vector<Rectangle>();
	Point tempPoint=null;      Point checkpointLL=null;   Point checkpointUU=null;   Point checkpointUL=null;   Point checkpointLU=null;
	boolean initUU=false;      boolean initLL=false;      boolean initLU=false;      boolean initUL=false;	
		
	  for(int k=0; k<pointsarray.length; k++){
		       tempPoint=pointsarray[k].clone();
			 
		
			 
/*Initialization phase for checkpoint. First point, of each quadrant, of map function.*/ 
	    	 if(!initUU && quadrant(tempPoint, queryPoint)==4){	 
		    	 checkpointUU= tempPoint.clone();    
		    	 initUU=true;
		    	 mapList.add(tempPoint.clone());
	    	 }else if(!initLL && quadrant( tempPoint, queryPoint)==1){	 
		    	 checkpointLL= tempPoint.clone();     	    		 
			     initLL=true;
			     mapList.add(tempPoint.clone());    	 
			}else if(!initLU && quadrant( tempPoint, queryPoint)==3){	 
				 checkpointLU= tempPoint.clone();    
			     initLU=true;
			     mapList.add(tempPoint.clone());	 
			}else if(!initUL && quadrant( tempPoint, queryPoint)==2){	 
				 checkpointUL= tempPoint.clone();     
			     initUL=true;
			     mapList.add(tempPoint.clone());		 
			}else{
				 /*Compute the dominance between checkpoints and the new point.*/
				  dominance=false;
				  int quadrant=-1;
				  switch (quadrant(tempPoint, queryPoint)) {
		    		    case 1: dominance = skylineReverseDominate( checkpointLL , tempPoint, queryPoint); quadrant=1; break;  //minmin
		    		    case 2: dominance = skylineReverseDominate( checkpointUL , tempPoint, queryPoint); quadrant=2; break;  //maxmin
		    		    case 3: dominance = skylineReverseDominate( checkpointLU , tempPoint, queryPoint); quadrant=3; break;  //minmax
		    		    case 4: dominance = skylineReverseDominate( checkpointUU , tempPoint, queryPoint); quadrant=4; break;  //maxmax
		    		    default: 
		    		    	 throw new RuntimeException("Unknown quadrant d: "+quadrant(tempPoint, queryPoint));
				   }
				  /* End */

		       if(!(dominance)){
     	    	  if(     (quadrant==1)&&(tempPoint.distanceToEuclidean(queryPoint.x,queryPoint.y)<checkpointLL.distanceToEuclidean(queryPoint.x,queryPoint.y))) {checkpointLL=tempPoint.clone();}
     	    	  else if((quadrant==2)&&(tempPoint.distanceToEuclidean(queryPoint.x,queryPoint.y)<checkpointUL.distanceToEuclidean(queryPoint.x,queryPoint.y))) {checkpointUL=tempPoint.clone();}
     	    	  else if((quadrant==3)&&(tempPoint.distanceToEuclidean(queryPoint.x,queryPoint.y)<checkpointLU.distanceToEuclidean(queryPoint.x,queryPoint.y))) {checkpointLU=tempPoint.clone();}
     	    	  else if((quadrant==4)&&(tempPoint.distanceToEuclidean(queryPoint.x,queryPoint.y)<checkpointUU.distanceToEuclidean(queryPoint.x,queryPoint.y))) {checkpointUU=tempPoint.clone();}
     	    	  else if((quadrant==-1)){ LOG.error("Unknown quadrant .");}
     	    	
     	    	  mapList.add(tempPoint.clone());
     	    	  
     	        }else{
     	        	//reject
     	        }  		       
			}//End if for checkpoints.
		}//End While for all points.

	  	mapList.sort(new Comparator <Point>(){
	 		 public int compare(Point p1, Point p2){   				 
	 			 if(p1.distanceToEuclidean(queryPoint.x,queryPoint.y)<p2.distanceToEuclidean(queryPoint.x,queryPoint.y)) return -1;
	 			 if(p1.distanceToEuclidean(queryPoint.x,queryPoint.y)>p2.distanceToEuclidean(queryPoint.x,queryPoint.y)) return  1;	
	 				return 0;
	 	}});
  	
		/* 
		* Phase two: Run the algorithm for all the sorted points Refined points.
		*/
		    
		for(int j=0; j<mapList.size(); j++){
			  Point point=mapList.get(j).clone();
		
				 /*Compute the dominance between checkpoints and the new point.*/
				  dominance=false;
				  int quadrant=-1;
				  switch (quadrant(tempPoint, queryPoint)) {
		    		    case 1: dominance = skylineReverseDominate( checkpointLL , point, queryPoint); quadrant=1; break;  //minmin
		    		    case 2: dominance = skylineReverseDominate( checkpointUL , point, queryPoint); quadrant=2; break;  //maxmin
		    		    case 3: dominance = skylineReverseDominate( checkpointLU , point, queryPoint); quadrant=3; break;  //minmax
		    		    case 4: dominance = skylineReverseDominate( checkpointUU , point, queryPoint); quadrant=4; break;  //maxmax
		    		    default: throw new RuntimeException("Unknown quadrant d: "+quadrant(point, queryPoint));
				   }
				  /* End */
			  
		
		    	if(dominance){ 
		    		//reject
		    	 }else{
		    		 boolean dominatedSkyList = false; 
					 for (Point skypoint : globalskyList) {
				    	  dominatedSkyList=(skylineReverseDominate(skypoint , point, queryPoint));
					       if (dominatedSkyList) {
		  			        	break;
					       }
					  }
		    		 if (!dominatedSkyList){
		    			 globalskyList.add(point);;
				    } 
		    	 }   		
			 }//for

		globalskyList.sort(new Comparator <Point>(){
				 public int compare(Point p1, Point p2){   				 
					 if(p1.distanceToEuclidean(queryPoint.x,queryPoint.y)<p2.distanceToEuclidean(queryPoint.x,queryPoint.y)) return -1;
					 if(p1.distanceToEuclidean(queryPoint.x,queryPoint.y)>p2.distanceToEuclidean(queryPoint.x,queryPoint.y)) return  1;	
						return 0;
			}});

		for(int j=0; j<globalskyList.size(); j++){	
			Point point =globalskyList.get(j).clone();
		    Rectangle rangeRect=null;
		    
		    if(quadrant( point, queryPoint)==4){	
			     rangeRect = new Rectangle();
			     rangeRect.x1 = queryPoint.x;
			     rangeRect.y1 = queryPoint.y;
			     rangeRect.x2 = queryPoint.x + Math.abs( point.x - queryPoint.x)*2;  
			     rangeRect.y2 = queryPoint.y + Math.abs( point.y - queryPoint.y)*2; 
			 }else if(quadrant( point, queryPoint)==1){	 
			     rangeRect = new Rectangle();
				     rangeRect.x1 = queryPoint.x - Math.abs( queryPoint.x -  point.x )*2;  
				     rangeRect.y1 = queryPoint.y - Math.abs( queryPoint.y -  point.y )*2; 
				     rangeRect.x2 = queryPoint.x;
				     rangeRect.y2 = queryPoint.y;
			 }else if(quadrant( point, queryPoint)==3){	  
			     rangeRect = new Rectangle();
			     rangeRect.x1 = queryPoint.x - Math.abs( queryPoint.x - point.x )*2; 
			     rangeRect.y1 = queryPoint.y; 
			     rangeRect.x2 = queryPoint.x;
			     rangeRect.y2 = queryPoint.y + Math.abs(point.y -queryPoint.y )*2; 
			 }else if(quadrant( point, queryPoint)==2){	
				  rangeRect = new Rectangle();
				  rangeRect.x1 = queryPoint.x;
				  rangeRect.y1 = queryPoint.y - Math.abs( queryPoint.y - point.y )*2; 
				  rangeRect.x2 = queryPoint.x + Math.abs( point.x - queryPoint.x )*2;  
				  rangeRect.y2 = queryPoint.y;
			 }
		    
		    RangeRectangles.add(rangeRect);
		}

		Rectangle[] queryRanges = new Rectangle[globalskyList.size()];
		
		for(int j=0; j<RangeRectangles.size(); j++){
			queryRanges[j]=RangeRectangles.get(j).clone();
		}
		
		final Path inPath = params.getInputPath();
		final Path outPath = params.getOutputPath();
		//final Rectangle[] queryRanges = params.getShapes("rect", new Rectangle());
		
		// All running jobs
		final Vector<Long> resultsCounts = new Vector<Long>();
		Vector<Job> jobs = new Vector<Job>();
		Vector<Thread> threads = new Vector<Thread>();
		
		for (int i = 0; i < queryRanges.length; i++) {
		  final OperationsParams queryParams = new OperationsParams(params);
		  OperationsParams.setShape(queryParams, "rect", queryRanges[i]);
  
    // Run in local mode
    final Rectangle queryRange = queryRanges[i];
    final Shape shape = queryParams.getShape("shape");
    final Path output = outPath == null ? null :
      (queryRanges.length == 1 ? outPath : new Path(outPath, String.format("%05d", i)));
    Thread thread = new Thread() {
      @Override
      public void run() {
        FSDataOutputStream outFile = null;
        final byte[] newLine = System.getProperty("line.separator", "\n").getBytes();
        try {
          ResultCollector<Shape> collector = null;
          if (output != null) {
            FileSystem outFS = output.getFileSystem(queryParams);
            final FSDataOutputStream foutFile = outFile = outFS.create(output);
            collector = new ResultCollector<Shape>() {
              final Text tempText = new Text2();
              @Override
              public synchronized void collect(Shape r) {
                try {
                  tempText.clear();
                  r.toText(tempText);
                  foutFile.write(tempText.getBytes(), 0, tempText.getLength());
                  foutFile.write(newLine);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            };
          } else {
            outFile = null;
          }
          long resultCount = rangeQueryLocal(inPath, queryRange, shape, queryParams, collector);
          resultsCounts.add(resultCount);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } finally {
          try {
            if (outFile != null)
              outFile.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    };
    thread.start();
    threads.add(thread);
}
    
    while (!threads.isEmpty()) {
        try {
          Thread threadtest = threads.firstElement();
          threadtest.join();
          threads.remove(0);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
     
	  int size=0;
	  for (int i=0; i<resultsCounts.size(); i++){
		 if(resultsCounts.get(i)==0 || resultsCounts.get(i)==1){
			 size++;
		 }
	  }
	  
    Point[] pointsarrayreturned = new Point[size];
    int pos=0;
    for(int j=0; j<globalskyList.size(); j++){
    	if(resultsCounts.get(j)==0 || resultsCounts.get(j)==1){
    		pointsarrayreturned[pos]=globalskyList.get(j).clone();
    		pos++;
    	}
    }
    
     return pointsarrayreturned;  
  }
  
/**
 * Runs a range query on the local machine (no MapReduce) and the output is
 * streamed to the provided result collector. The query might run in parallel
 * which makes it necessary to design the result collector to accept parallel
 * calls to the method {@link ResultCollector#collect(Object)}.
 * You can use {@link ResultCollectorSynchronizer} to synchronize calls to
 * your ResultCollector if you cannot design yours to be thread safe.
 * @param inPath
 * @param queryRange
 * @param shape
 * @param params
 * @param output
 * @return
 * @throws IOException
 * @throws InterruptedException
 */
public static <S extends Shape> long rangeQueryLocal(Path inPath,
    final Shape queryRange, final S shape,
    final OperationsParams params, final ResultCollector<S> output) throws IOException, InterruptedException {
  // Set MBR of query shape in job configuration to work with the spatial filter
  OperationsParams.setShape(params, SpatialInputFormat3.InputQueryRange, queryRange.getMBR());
  // 1- Split the input path/file to get splits that can be processed independently
  final SpatialInputFormat3<Rectangle, S> inputFormat = new SpatialInputFormat3<Rectangle, S>();
  Job job = Job.getInstance(params);
  SpatialInputFormat3.setInputPaths(job, inPath);
  final List<InputSplit> splits = inputFormat.getSplits(job);
  
  // 2- Process splits in parallel
  List<Long> results = Parallel.forEach(splits.size(), new RunnableRange<Long>() {
    @Override
    public Long run(int i1, int i2) {
      long results = 0;
      for (int i = i1; i < i2; i++) {
        try {
          FileSplit fsplit = (FileSplit) splits.get(i);
          final RecordReader<Rectangle, Iterable<S>> reader = inputFormat.createRecordReader(fsplit, null);
          if (reader instanceof SpatialRecordReader3) {
            ((SpatialRecordReader3)reader).initialize(fsplit, params);
          } else if (reader instanceof RTreeRecordReader3) {
            ((RTreeRecordReader3)reader).initialize(fsplit, params);
          } else if (reader instanceof HDFRecordReader) {
            ((HDFRecordReader)reader).initialize(fsplit, params);
          } else {
            throw new RuntimeException("Unknown record reader");
          }
          while (reader.nextKeyValue()) {
            Iterable<S> shapes = reader.getCurrentValue();
            for (Shape s : shapes) {
              results++;
              if (output != null)
                output.collect((S) s);
            }
          }
          reader.close();
        } catch (IOException e) {
          LOG.error("Error processing split "+splits.get(i), e);
        } catch (InterruptedException e) {
          LOG.error("Error processing split "+splits.get(i), e);
        }
      }
      return results;
    }
  });
  long totalResultSize = 0;
  for (long result : results)
    totalResultSize += result;
  return totalResultSize;
}
  
  public static void ReverseskylineLocal(Path inFile, Path outFile,
	      final OperationsParams params) throws IOException, InterruptedException {
	    if (params.getBoolean("mem", false))
	      MemoryReporter.startReporting();
	    // 1- Split the input path/file to get splits that can be processed
	    // independently
	    final SpatialInputFormat3<Rectangle, Point> inputFormat =new SpatialInputFormat3<Rectangle, Point>();
	    Job job = Job.getInstance(params);
	    SpatialInputFormat3.setInputPaths(job, inFile);
	    final List<InputSplit> splits = inputFormat.getSplits(job);
	    final Direction dir = params.getDirection("dir", Direction.MaxMax);
	    
	    // 2- Read all input points in memory
	    LOG.info("Reading points from "+splits.size()+" splits");
	    List<Point[]> allLists = Parallel.forEach(splits.size(), new RunnableRange<Point[]>() {
	      @Override
	      public Point[] run(int i1, int i2) {
	        try {
	          List<Point> finalPoints = new ArrayList<Point>();
	          final int MaxSize = 1500000;
	          Point[] points = new Point[MaxSize];
	          int size = 0;
	          for (int i = i1; i < i2; i++) {
	            org.apache.hadoop.mapreduce.lib.input.FileSplit fsplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit) splits.get(i);
	            final RecordReader<Rectangle, Iterable<Point>> reader =
	                inputFormat.createRecordReader(fsplit, null);
	            if (reader instanceof SpatialRecordReader3) {
	              ((SpatialRecordReader3)reader).initialize(fsplit, params);
	            } else if (reader instanceof RTreeRecordReader3) {
	              ((RTreeRecordReader3)reader).initialize(fsplit, params);
	            } else if (reader instanceof HDFRecordReader) {
	              ((HDFRecordReader)reader).initialize(fsplit, params);
	            } else {
	              throw new RuntimeException("Unknown record reader");
	            }
	            while (reader.nextKeyValue()) {
	              Iterable<Point> pts = reader.getCurrentValue();
	              for (Point p : pts) {
	                points[size++] = p.clone();
	                if (size >= points.length) {
	                  // Perform Skyline and write the result to finalPoints
	                  Point[] skylinePoints = ReverseskylineInMemory(points, dir, params);
	                  for (Point skylinePoint : skylinePoints)
	                    finalPoints.add(skylinePoint);
	                  size = 0; // reset
	                }
	              }
	            }
	            reader.close();
	          }
	          while (size-- > 0)
	            finalPoints.add(points[size]);
	          return finalPoints.toArray(new Point[finalPoints.size()]);
	        } catch (IOException e) {
	          e.printStackTrace();
	        } catch (InterruptedException e) {
	          e.printStackTrace();
	        }
	        return null;
	      }
	    }, params.getInt("parallel", Runtime.getRuntime().availableProcessors()));
	    
	    int totalNumPoints = 0;
	    for (Point[] list : allLists)
	      totalNumPoints += list.length;
	    
	    Point[] allPoints = new Point[totalNumPoints];
	    int pointer = 0;

	    for (Point[] list : allLists) {
	      System.arraycopy(list, 0, allPoints, pointer, list.length);
	      pointer += list.length;
	    }
	    allLists.clear(); // To the let the GC collect it

	    Point[] skyline = ReverseskylineInMemory(allPoints, dir, params);

	    if (outFile != null) {
	      if (params.getBoolean("overwrite", false)) {
	        FileSystem outFs = outFile.getFileSystem(new Configuration());
	        outFs.delete(outFile, true);
	      }
	      
	      
	      GridRecordWriter<Point> out = new GridRecordWriter<Point>(outFile, null, null, null);
	      for (Point pt : skyline) {
	        out.write(NullWritable.get(), pt);
	      }
	      out.close(null);
	    }    

	  }
  
  
  
  public static class ReverseSkylineFilter extends DefaultBlockFilter {
	      
    private Direction dir;
    private Point queryPoint;
    
    @Override
    public void configure(Configuration conf) {;
      super.configure(conf);
      dir = OperationsParams.getDirection(conf, "dir", Direction.MaxMax);
      queryPoint=OperationsParams.getQueryPoint(conf, "query", new Point(0,0));
    }
    
    public static boolean skylineRectDominateReverse(Rectangle r1, Rectangle r2, Point q, boolean compact) {
 	   /*this function assumes that partition does not intersect axis.*/
		  /*Find the center of mbr and identify quadrant.*/
		  double x= r1.getCenterPoint().x-q.x;
		  double y= r1.getCenterPoint().y-q.y;
		  
		  String quadrant="MinMin";
/*Erroneous states occur but handled by the other functions. */
		  if(x>0 && y>0){
			  quadrant="MaxMax";
		  }else if(x>0 && y<0){
			  quadrant="MaxMin";
		  }else if(x<0 && y>0){
			  quadrant="MinMax";
		  }else if(x<0 && y<0){
			  quadrant="MinMin";
		  }

		  boolean sameguadrant= ((((r1.x1-q.x)*(r2.x1-q.x)>0)&&((r1.x2-q.x)*(r2.x2-q.x)>0)) &&(((r1.y1-q.y)*(r2.y1-q.y)>0)&&((r1.y2-q.y)*(r2.y2-q.y)>0)));

		  /*Based on quadrant return dominance result.*/
		  switch (quadrant) {   // previously was sameguadrant
				  case "MinMin": return compact ? (( Math.abs(r1.x2-q.x) <= Math.abs(r2.x2-q.x) ) && (  Math.abs(r1.y1-q.y) <= Math.abs(r2.y2-q.y) ) || 
				        /*maxmax*/                 ( Math.abs(r1.x1-q.x) <= Math.abs(r2.x2-q.x) ) && (  Math.abs(r1.y2-q.y) <= Math.abs(r2.y2-q.y) ))&& sameguadrant :
				                                   ( Math.abs(r1.x1-q.x) <= Math.abs(r2.x2-q.x)   &&    Math.abs(r1.y1-q.y) <= Math.abs(r2.y2-q.y) ) && sameguadrant ; 
				  
				  case "MinMax": return compact ? (( Math.abs(r1.x2-q.x) <= Math.abs(r2.x2-q.x) ) && (  Math.abs(r1.y2-q.y) <= Math.abs(r2.y1-q.y) ) || 
				        /*maxmin*/                 ( Math.abs(r1.x1-q.x) <= Math.abs(r2.x2-q.x) ) && (  Math.abs(r1.y1-q.y) <= Math.abs(r2.y1-q.y) ))&& sameguadrant :
				                                   ( Math.abs(r1.x1-q.x) <= Math.abs(r2.x2-q.x)   &&    Math.abs(r1.y2-q.y) <= Math.abs(r2.y1-q.y) ) && sameguadrant ;
				 
				  case "MaxMin": return compact ? (( Math.abs(r1.x2-q.x) <= Math.abs(r2.x1-q.x) ) && (  Math.abs(r1.y2-q.y) <= Math.abs(r2.y2-q.y) ) || 
				          /*minmax*/               ( Math.abs(r1.x1-q.x) <= Math.abs(r2.x1-q.x) ) && (  Math.abs(r1.y2-q.y) <= Math.abs(r2.y2-q.y) ))&& sameguadrant :
				                                   ( Math.abs(r1.x2-q.x) <= Math.abs(r2.x1-q.x)   &&    Math.abs(r1.y1-q.y) <= Math.abs(r2.y2-q.y) ) && sameguadrant ;   
				  
				  case "MaxMax": return compact ? (( Math.abs(r1.x2-q.x) <= Math.abs(r2.x1-q.x) ) && (  Math.abs(r1.y1-q.y) <= Math.abs(r2.y1-q.y) ) || 
			             /*minmin*/                ( Math.abs(r1.x1-q.x) <= Math.abs(r2.x1-q.x) ) && (  Math.abs(r1.y2-q.y) <= Math.abs(r2.y1-q.y) ))&& sameguadrant :
			                                       ( Math.abs(r1.x2-q.x) <= Math.abs(r2.x1-q.x)   &&    Math.abs(r1.y2-q.y) <= Math.abs(r2.y1-q.y) ) && sameguadrant ;  
				 default: throw new RuntimeException("Unknown direction: "+quadrant);
		  }  
	  }
    
    @Override
    public void selectCells(GlobalIndex<Partition> gIndex,ResultCollector<Partition> output) {

    	      Vector<Partition> non_dominated_partitions = new Vector<Partition>();
    	      Vector<Partition> mandatory_partitions     = new Vector<Partition>();
    	      Vector<Partition> rest_partitions = new Vector<Partition>();
    	      Vector<Rectangle> refirement_rects = new Vector<Rectangle>();
    	      
    	      long totalpoints=0;
    	      double avgpermbr=0;
    	      
	    	  /*Construct Axis - X-Line, Y-Line.*/
	    	  boolean hasBeenAdded=false;
	          Rectangle axesXLine = new Rectangle(); 
	          Rectangle axesYLine = new Rectangle();
	          
	          axesXLine.x1 = Double.MIN_VALUE;//0;
	          axesXLine.y1 = queryPoint.y;
	          axesXLine.x2 = Double.MAX_VALUE;//1000000;
	          axesXLine.y2 = queryPoint.y;
	          
	          axesYLine.x1 = queryPoint.x;
	          axesYLine.y1 = Double.MIN_VALUE;//0;
	          axesYLine.x2 = queryPoint.x;
	          axesYLine.y2 = Double.MAX_VALUE;//1000000;
    	      
    	      
/* 1st refinement Step
* Identify mandatory Partition which contains query point or intersect axes.
* */
    	      for (Partition pStart : gIndex){ 
    	    	  /*Partition that contains query point*/
    	    	  if(pStart.contains(queryPoint)){ 
    	    		  mandatory_partitions.add(pStart); 
    	    		  hasBeenAdded=true;
    	    	  }else if(pStart.isIntersected(axesXLine)){ /*Check if partitions intersect.*/
    	    		  if(!mandatory_partitions.contains(pStart)){
    	    			  mandatory_partitions.add(pStart); 
    	    			  hasBeenAdded=true;
    	    		  }
    	    	  }else if(pStart.isIntersected(axesYLine)){
    	    		  if(!mandatory_partitions.contains(pStart)){
    	    			  mandatory_partitions.add(pStart); 
    	    			  hasBeenAdded=true;
    	    		  }
    	    	  }
    	    	  
    	    	  /*If not a mandatory partition add to a list for further process.*/
    	    	  if(!hasBeenAdded){
    	    		  rest_partitions.add(pStart); 
    	    	  }
    	    	  hasBeenAdded=false;
    	      }

/* 2nd refinement step
 * Check for dominance only the rest partitions, as the dominance 
 * of mandatory partitions is not guaranteed.
 * */   	      
    	      for (Partition p : rest_partitions) {   
    		        boolean dominated = false;
    		        int i = 0;
    		        while (!dominated && i < non_dominated_partitions.size()) {
    			         Partition p2 = non_dominated_partitions.get(i);
    			         /*this function assumes that partition does not intersect axis.*/
    			         dominated = skylineRectDominateReverse(p2, p, queryPoint, gIndex.isCompact());
    			         
    			          /* Check if the new partition dominates the previously selected one.*/
    			         /*this function assumes that partition does not intersect axis.*/
    			          if (skylineRectDominateReverse(p, p2, queryPoint, gIndex.isCompact())) {
    			        	 // p2 is no longer non-dominated
    			        	 non_dominated_partitions.remove(i);
    			          } else {
    			            // Skip to next non-dominated partition
    			            i++;
    			          }
    		        }
    		        
    		        if (!dominated) {
    		            non_dominated_partitions.add(p);   
    		        } else{ //Remove partition.
    		        }
    	      }
 
    	      for (Partition p : non_dominated_partitions) {
    	    	   output.collect(p);
    	      }
    	      
    	      for (Partition p : mandatory_partitions) {
    	    	  output.collect(p);
    	      }
    }
  }

  

  public static class IdentityMapper extends MapReduceBase implements Mapper<Rectangle, ShapeIterator, NullWritable, Point> {
	    
	 private Direction dir;
	 private Point queryPoint;
	 
	 @Override
	 public void configure(JobConf job) {
	    super.configure(job);
	    dir = OperationsParams.getDirection(job, "dir", Direction.MaxMax);
	    queryPoint=OperationsParams.getQueryPoint(job, "query", new Point(0,0));
	  }

	 
	@Override
    public void map(Rectangle dummy, ShapeIterator points,OutputCollector<NullWritable, Point> output, Reporter reporter) throws IOException {

/* Identify Intersections with axis, 
 * or if the query point belongs to this 
 * partition.
 * */
		 Boolean dominance;
	     Rectangle axesXLine = new Rectangle();
	     Rectangle axesYLine = new Rectangle(); 
	     
	     axesXLine.x1 = Double.MIN_VALUE; axesXLine.y1 = queryPoint.y; axesXLine.x2 = Double.MAX_VALUE;  axesXLine.y2 = queryPoint.y;
	     axesYLine.x1 = queryPoint.x; axesYLine.y1 = Double.MIN_VALUE; axesYLine.x2 = queryPoint.x; axesYLine.y2 = Double.MAX_VALUE;
	     
	  	 if(dummy.isIntersected(axesXLine)){ System.out.println("\t\tCall:  intersect X-line.");}      
		 if(dummy.isIntersected(axesYLine)){ System.out.println("\t\tCall:  intersect Y-line.");}
  	  	/* End */
					  
/* Phase one: Identify checkpoints and place points in map to sort them.
 * Place all point of map to a list and sort them based on mindist from query point
 *  */
   	  
  		Vector<Point> mapList = new Vector<Point>();
  		Vector<Point> skyList = new Vector<Point>();
  		Point tempPoint=null;      Point checkpointLL=null;   Point checkpointUU=null;   Point checkpointUL=null;   Point checkpointLU=null;
  		boolean initUU=false;      boolean initLL=false;      boolean initLU=false;      boolean initUL=false;	
  		
  		while (points.hasNext()) {
			 tempPoint=(Point)points.next().clone();
 			 
 		
 			 
 /*Initialization phase for checkpoint. First point, of each quadrant, of map function.*/ 
 	    	 if(!initUU && quadrant(tempPoint, queryPoint)==4){	 
 		    	 checkpointUU= tempPoint.clone();    
 		    	 initUU=true;
 		    	 mapList.add(tempPoint.clone());
 	    	 }else if(!initLL && quadrant( tempPoint, queryPoint)==1){	 
 		    	 checkpointLL= tempPoint.clone();     	    		 
 			     initLL=true;
 			     mapList.add(tempPoint.clone());    	 
 			}else if(!initLU && quadrant( tempPoint, queryPoint)==3){	 
 				 checkpointLU= tempPoint.clone();    
 			     initLU=true;
 			     mapList.add(tempPoint.clone());	 
 			}else if(!initUL && quadrant( tempPoint, queryPoint)==2){	 
 				 checkpointUL= tempPoint.clone();     
 			     initUL=true;
 			     mapList.add(tempPoint.clone());		 
 			}else{
 			 
 	    	 
    		 
    		 /*Compute the dominance between checkpoints and the new point.*/
    		  dominance=false;
    		  int quadrant=-1;
    		  switch (quadrant(tempPoint, queryPoint)) {
	    		    case 1: dominance = skylineReverseDominate( checkpointLL , tempPoint, queryPoint); quadrant=1; break;  //minmin
	    		    case 2: dominance = skylineReverseDominate( checkpointUL , tempPoint, queryPoint); quadrant=2; break;  //maxmin
	    		    case 3: dominance = skylineReverseDominate( checkpointLU , tempPoint, queryPoint); quadrant=3; break;  //minmax
	    		    case 4: dominance = skylineReverseDominate( checkpointUU , tempPoint, queryPoint); quadrant=4; break;  //maxmax
	    		    default: 
	    		    	 throw new RuntimeException("Unknown quadrant d: "+quadrant(tempPoint, queryPoint));
    		   }
    		  /* End */

    		       if(!(dominance)){
	     	    	  if(     (quadrant==1)&&(tempPoint.distanceToEuclidean(queryPoint.x,queryPoint.y)<checkpointLL.distanceToEuclidean(queryPoint.x,queryPoint.y))) {checkpointLL=tempPoint.clone();}
	     	    	  else if((quadrant==2)&&(tempPoint.distanceToEuclidean(queryPoint.x,queryPoint.y)<checkpointUL.distanceToEuclidean(queryPoint.x,queryPoint.y))) {checkpointUL=tempPoint.clone();}
	     	    	  else if((quadrant==3)&&(tempPoint.distanceToEuclidean(queryPoint.x,queryPoint.y)<checkpointLU.distanceToEuclidean(queryPoint.x,queryPoint.y))) {checkpointLU=tempPoint.clone();}
	     	    	  else if((quadrant==4)&&(tempPoint.distanceToEuclidean(queryPoint.x,queryPoint.y)<checkpointUU.distanceToEuclidean(queryPoint.x,queryPoint.y))) {checkpointUU=tempPoint.clone();}
	     	    	  else if((quadrant==-1)){ LOG.error("Unknown quadrant .");}
	     	    	  mapList.add(tempPoint.clone());
	     	        }else{
	     	        	//reject
	     	        }
     		       
 			}//End if for checkpoints.
  		}//End While for all points.
  		
  		if(!(checkpointLL==null)) {output.collect( NullWritable.get(), (Point)checkpointLL);}
  		if(!(checkpointUL==null)) {output.collect( NullWritable.get(), (Point)checkpointUL);}
  		if(!(checkpointLU==null)) {output.collect( NullWritable.get(), (Point)checkpointLU);}
  		if(!(checkpointUU==null)) {output.collect( NullWritable.get(), (Point)checkpointUU);}

      	mapList.sort(new Comparator <Point>(){
     		 public int compare(Point p1, Point p2){   				 
     			 if(p1.distanceToEuclidean(queryPoint.x,queryPoint.y)<p2.distanceToEuclidean(queryPoint.x,queryPoint.y)) return -1;
     			 if(p1.distanceToEuclidean(queryPoint.x,queryPoint.y)>p2.distanceToEuclidean(queryPoint.x,queryPoint.y)) return  1;	
     				return 0;
     	}});

      	
 /* 
  * Phase two: Run the algorithm for all the sorted points Refined points.
  */
	    
    for(int j=0; j<mapList.size(); j++){
    	  Point point=mapList.get(j).clone();

   		 /*Compute the dominance between checkpoints and the new point.*/
  		  dominance=false;
  		  int quadrant=-1;
  		  switch (quadrant(tempPoint, queryPoint)) {
	    		    case 1: dominance = skylineReverseDominate( checkpointLL , point, queryPoint); quadrant=1; break;  //minmin
	    		    case 2: dominance = skylineReverseDominate( checkpointUL , point, queryPoint); quadrant=2; break;  //maxmin
	    		    case 3: dominance = skylineReverseDominate( checkpointLU , point, queryPoint); quadrant=3; break;  //minmax
	    		    case 4: dominance = skylineReverseDominate( checkpointUU , point, queryPoint); quadrant=4; break;  //maxmax
	    		    default: 
	    		    	 throw new RuntimeException("Unknown quadrant d: "+quadrant(point, queryPoint));
  		   }
  		  /* End */
    	  

	    	if(dominance){ 
	    		//reject
	    	 }else{
	    		 boolean dominatedSkyList = false; 
				 for (Point skypoint : skyList) {
			    	  dominatedSkyList=(skylineReverseDominate(skypoint , point, queryPoint));
				       if (dominatedSkyList) {
	  			        	break;
				       }
				  }
	    		 if (!dominatedSkyList){
		  			    skyList.add(point);
		  			    output.collect( NullWritable.get(), (Point)point);
			    } 
	    	 }	
    	 }//for
      }//mapper
  }//class


  public class RangeFilter extends DefaultBlockFilter {
	    private Direction dir;
	    private Point queryPoint;
	    private Rectangle queryRange;
	    
	    @Override
	    public void configure(Configuration conf) {
	      super.configure(conf);
	      dir = OperationsParams.getDirection(conf, "dir", Direction.MaxMax);
	      queryPoint=OperationsParams.getQueryPoint(conf, "query", new Point(0,0));
	    }
	    
	  @Override  
	  public void selectCells(GlobalIndex<Partition> gIndex,ResultCollector<Partition> output) {	
		  	    gIndex.rangeQuery(queryRange, output);	
	  }
	}
  

  public static class RangeQueryMapper extends MapReduceBase implements Mapper<Rectangle, Iterable<Shape>, NullWritable, Shape> {
	     private Direction dir;
		 private Point queryPoint;
		 
		 @Override
		 public void configure(JobConf job) {
		    super.configure(job);
		    dir = OperationsParams.getDirection(job, "dir", Direction.MaxMax);
		    queryPoint=OperationsParams.getQueryPoint(job, "query", new Point(0,0));
		  }
	  
	@Override
	public void map (Rectangle dummy, Iterable<Shape> value, OutputCollector<NullWritable, Shape> output, Reporter reporter) throws IOException {   
		  NullWritable dummyKey = NullWritable.get();
		  for (Shape s : value) {
		    output.collect(dummyKey, s);		    
		  }
		}
} 
  
  public static class ReverseSkylineReducer extends MapReduceBase implements Reducer<NullWritable,Point,NullWritable,Point> {  
	  
    private Direction dir;
    private Point queryPoint;
    
    @Override
    public void configure(JobConf job) {
      super.configure(job);
      dir = OperationsParams.getDirection(job, "dir", Direction.MaxMax);
	  queryPoint=OperationsParams.getQueryPoint(job, "query", new Point(0,0));
    }

    @Override
    public void reduce(NullWritable dummy, Iterator<Point> points, OutputCollector<NullWritable, Point> output, Reporter reporter) throws IOException {
 
        Vector <Point> vpoints = new Vector <Point>();
        Vector<Point> skyList = new Vector<Point>();
        
    	while (points.hasNext()) {  
	        vpoints.add(points.next().clone()); 
	    }//while
   	
    	vpoints.sort(new Comparator <Point>(){
    		 public int compare(Point p1, Point p2){   				 
    			 if(p1.distanceToEuclidean(queryPoint.x,queryPoint.y)<p2.distanceToEuclidean(queryPoint.x,queryPoint.y)) return -1;
    			 if(p1.distanceToEuclidean(queryPoint.x,queryPoint.y)>p2.distanceToEuclidean(queryPoint.x,queryPoint.y)) return  1;	
    				return 0;
    	}});

	     Rectangle rangeRect;
  	     for(Point point : vpoints){ 	 
    		 // Dominance list
    		 boolean dominatedSkyList = false; 
			 for (Point skypoint : skyList) {
		    	 dominatedSkyList=(skylineReverseDominate(skypoint , point, queryPoint));
			     if (dominatedSkyList) {
  			       	break;
			     }
			  }
			 		 
			 rangeRect=null;
			 if (!dominatedSkyList){
			  			skyList.add(point);
			  			output.collect( NullWritable.get(), (Point)point);
			 }else{
			 }
			 
  	     }//for vpoints
  	  }
  }
  
  
  public static Job rangeQueryMapReduce(Path inFile, Path outFile,
	      OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
	    // Use the built-in range filter of the input format
	    params.set(SpatialInputFormat3.InputQueryRange, params.get("rect"));
	    // Use multithreading in case it is running locally
	    params.setInt(LocalJobRunner.LOCAL_MAX_MAPS, Runtime.getRuntime().availableProcessors());
	    
	    Job job = new Job(params, "RangeQuery");
	    job.setJarByClass(RangeQuery.class);
	    job.setNumReduceTasks(0);
	    
	    job.setInputFormatClass(SpatialInputFormat3.class);
	    SpatialInputFormat3.setInputPaths(job, inFile);
	    
	    job.setMapperClass(RangeQueryMap.class);
	    System.err.println("outFile1: "+outFile);
	    
	    if (params.getBoolean("output", true) && outFile != null) {
	      System.err.println("outFile2: "+outFile);
	      job.setOutputFormatClass(TextOutputFormat3.class);
	      TextOutputFormat3.setOutputPath(job, outFile);
	    } else {
	      // Skip writing the output for the sake of debugging
	      job.setOutputFormatClass(NullOutputFormat.class);
	    }
	    // Submit the job
	    if (!params.getBoolean("background", false)) {
	      job.waitForCompletion(false);
	    } else {
	      job.submit();
	    }
	    return job;
	  }
  
  
  
  private static void ReverseskylineMapReduce(Path inFile, Path userOutPath,OperationsParams params) throws IOException, ClassNotFoundException, InterruptedException {
	System.out.println("\t\tCall: skylineMapReduce");
    JobConf job = new JobConf(params, ReverseSkylineSRSAS.class);
    Path outPath = userOutPath;
    FileSystem outFs = (userOutPath == null ? inFile : userOutPath).getFileSystem(job);
    Shape shape = params.getShape("shape");
    Point queryPoint=OperationsParams.getQueryPoint(job, "query", new Point(0,0));
    if (outPath == null) {
      do {
          outPath = new Path(inFile.toUri().getPath()+".skyline_"+(int)(Math.random() * 1000000));
      	 }while (outFs.exists(outPath));
    }
    
    job.setJobName("ReverseSkyline");
    job.setClass(SpatialSite.FilterClass, ReverseSkylineFilter.class, BlockFilter.class);
    job.setJarByClass(ReverseSkylineSRSAS.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(ReverseSkylineReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(shape.getClass());
    job.setInputFormat(ShapeIterInputFormat.class);
    ShapeInputFormat.addInputPath(job, inFile);
    job.setOutputFormat(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, outPath);
    RunningJob runningJob = JobClient.runJob(job);

    if (userOutPath == null) outFs.delete(outPath, true);

    Configuration configuration = new Configuration();
   
    Path  pt = new Path(outPath.toUri().getPath()+"/part-00000");
    BufferedReader br=new BufferedReader(new InputStreamReader(outFs.open(pt)));
    Vector<Rectangle> RangeRectangles= new Vector<Rectangle>();
    Vector<Point> GlobalPoints= new Vector<Point>();
    String line;
    line=br.readLine();
    
    while (line != null){
        String[] s = line.split(",");
        Point point =new Point(Double.valueOf(s[0]), Double.valueOf(s[1]));	
        Rectangle rangeRect=null;
        
        if(quadrant( point, queryPoint)==4){	
		     rangeRect = new Rectangle();
		     rangeRect.x1 = queryPoint.x;
		     rangeRect.y1 = queryPoint.y;
		     rangeRect.x2 = queryPoint.x + Math.abs( point.x - queryPoint.x)*2;  
		     rangeRect.y2 = queryPoint.y + Math.abs( point.y - queryPoint.y)*2; 
		 }else if(quadrant( point, queryPoint)==1){	 
		     rangeRect = new Rectangle();
   		     rangeRect.x1 = queryPoint.x - Math.abs( queryPoint.x -  point.x )*2;  
   		     rangeRect.y1 = queryPoint.y - Math.abs( queryPoint.y -  point.y )*2; 
   		     rangeRect.x2 = queryPoint.x;
   		     rangeRect.y2 = queryPoint.y;
		 }else if(quadrant( point, queryPoint)==3){	  
		     rangeRect = new Rectangle();
		     rangeRect.x1 = queryPoint.x - Math.abs( queryPoint.x - point.x )*2; 
		     rangeRect.y1 = queryPoint.y; 
		     rangeRect.x2 = queryPoint.x;
		     rangeRect.y2 = queryPoint.y + Math.abs(point.y -queryPoint.y )*2; 
		 }else if(quadrant( point, queryPoint)==2){	
			  rangeRect = new Rectangle();
			  rangeRect.x1 = queryPoint.x;
			  rangeRect.y1 = queryPoint.y - Math.abs( queryPoint.y - point.y )*2; 
			  rangeRect.x2 = queryPoint.x + Math.abs( point.x - queryPoint.x )*2;  
			  rangeRect.y2 = queryPoint.y;
		 }
        
        RangeRectangles.add(rangeRect);
        GlobalPoints.add(point);
        line=br.readLine();
    }
    

    // All running jobs
    final Vector<Long> resultsCounts = new Vector<Long>();
    Vector<Job> jobs = new Vector<Job>();
    String pathout = outPath.toString();  ; 
    final OperationsParams queryParams = new OperationsParams(params); 
    queryParams.setBoolean("output", false);
    queryParams.setBoolean("background", true); 
    

    for (int i = 0; i < RangeRectangles.size(); i++) {
        // Run in MapReduce mode
        OperationsParams.setShape(queryParams, "rect", RangeRectangles.get(i));
        outPath = new Path(inFile.toUri().getPath()+".range_"+(int)(Math.random() * 1000000));
        Job tempjob = rangeQueryMapReduce(inFile, outPath, queryParams);
        jobs.add(tempjob);
    }

    
    while (!jobs.isEmpty()) {
      Job firstJob = jobs.firstElement();
      firstJob.waitForCompletion(false);
      if (!firstJob.isSuccessful()) {
        for (int j = 1; j < jobs.size(); j++)
          jobs.get(j).killJob();
        System.exit(1);
      }
 
      org.apache.hadoop.mapreduce.Counters counters = firstJob.getCounters();
      org.apache.hadoop.mapreduce.Counter outputRecordCounter = counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS);
      resultsCounts.add(outputRecordCounter.getValue());
      jobs.remove(0);
    }
 
    Vector<Point> ReversePoints= new Vector<Point>();
        
    Point globalpoint=null;
    for (int u=0; u<RangeRectangles.size(); u++) { 
    	if(resultsCounts.get(u)==1) {
    		globalpoint=GlobalPoints.get(u).clone();
			 boolean dominatedRevList = false; 
			 for (Point reversepoint : ReversePoints) {
				 dominatedRevList=(skylineReverseDominate(reversepoint , globalpoint, queryPoint));
			     if (dominatedRevList) {
				       	break;
			     }
			  }

			 if (!dominatedRevList){
				 ReversePoints.add(globalpoint);
			 }
    	}
    }  
    
  }
  
  public static void skyline(Path inFile, Path outFile, OperationsParams params) throws IOException, InterruptedException, ClassNotFoundException {
    if (OperationsParams.isLocal(params, inFile)) {
	      //Process without MapReduce
	      ReverseskylineLocal(inFile, outFile, params);
    } else {
	      // Process with MapReduce
    	ReverseskylineMapReduce(inFile, outFile, params);
    }
  }
  
  private static void printUsage() {
    System.err.println("Computes the skyline of an input file of points");
    System.err.println("Parameters: (* marks required parameters)");
    System.err.println("<input file>: (*) Path to input file");
    System.err.println("<output file>: Path to output file");
    System.err.println("<direction (max-max|max-min|min-max|min-min)>: Direction of skyline (default is max-max)");
    System.err.println("-overwrite: Overwrite output file without notice");
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }
  
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	  
	  BasicConfigurator.configure();
	  Logger.getRootLogger().setLevel(Level.INFO);
	  
	 
    Path[] paths = params.getPaths();
    if (paths.length <= 1 && !params.checkInput()) {
      printUsage();
      System.exit(1);
    }
    if (paths.length >= 2 && !params.checkInputOutput()) {
      printUsage();
      System.exit(1);
    }
    Path inFile = params.getInputPath();
    Path outFile = params.getOutputPath();
    
    long t1 = System.currentTimeMillis();
    skyline(inFile, outFile, params);
    long t2 = System.currentTimeMillis();
    System.out.println("Total time: "+(t2-t1)+" millis");
  }
  
}