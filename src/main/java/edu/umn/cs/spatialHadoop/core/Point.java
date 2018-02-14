/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A class that holds coordinates of a point.
 * @author aseldawy
 *
 */
public class Point implements Shape, Comparable<Point> {
	public double x;
	public double y;
	public double distance;

		
	public Point() {
		this(0, 0);
	}
	
	public Point(double x, double y) {
	  set(x, y);
	  setdistance(x,y);
	}
	

	/**
	 * A copy constructor from any shape of type Point (or subclass of Point)
	 * @param s
	 */
  public Point(Point s) {   //ok
	  this.x = s.x;
	  this.y = s.y;
	  this.distance=s.distance;

    }

  public void set(double x, double y) { //ok
		this.x = x;
		this.y = y;
	}
  
  public void setdistance(double x, double y) { //ok
		this.distance=this.distanceTo(x,y);
	}
  

	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
	}

	public void readFields(DataInput in) throws IOException {
		this.x = in.readDouble();
		this.y = in.readDouble();
	}

	public int compareTo(Shape s) {
	  Point pt2 = (Point) s;

	  // Sort by id
	  double difference = this.x - pt2.x;
		if (difference == 0) {
			difference = this.y - pt2.y;
		}
		if (difference == 0)
		  return 0;
		return difference > 0 ? 1 : -1;
	}
	
	public boolean equals(Object obj) {
		if (obj == null) 
			return false;
		Point r2 = (Point) obj;
		return this.x == r2.x && this.y == r2.y;
	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		temp = Double.doubleToLongBits(this.x);
		result = (int) (temp ^ temp >>> 32);
		temp = Double.doubleToLongBits(this.y);
		result = 31 * result + (int) (temp ^ temp >>> 32);
		return result;
	}

	
	public double distanceTo(Point s) {
		double dx = s.x - this.x;
		double dy = s.y - this.y;
		return Math.sqrt(dx*dx+dy*dy);
	}
	

		
	public int distanceTo2(int x, int y) { //old
		return (int) Math.abs((( x/2 - this.x/2) + ( y/2 -  this.y/2)));
	}
	
	  @Override
	  public double distanceTo(double px, double py) { //old
	    double dx = x - px;
	    double dy = y - py;
	    return Math.sqrt(dx * dx + dy * dy);
	  }
	  	

	  public double distanceToEuclidean(double px, double py){ 
		    double dx = this.x - px;
		    double dy = this.y - py;
		    return Math.sqrt(dx * dx + dy * dy);
	  }
	
	 public double distanceToManchatan(double px, double py){ //same as distanceTo
			return  Math.abs(( (this.x - px/2) + (this.y - py/2) ));
	  }

	@Override
	public Point clone() {
	  return new Point(this.x, this.y);
	}

	/**
	 * Returns the minimal bounding rectangle of this point. This method returns
	 * the smallest rectangle that contains this point. For consistency with
	 * other methods such as {@link Rectangle#isIntersected(Shape)}, the rectangle
	 * cannot have a zero width or height. Thus, we use the method
	 * {@link Math#ulp(double)} to compute the smallest non-zero rectangle that
	 * contains this point. In other words, for a point <code>p</code> the
	 * following statement should return true.
	 * <code>p.getMBR().isIntersected(p);</code>
	 */
  @Override
  public Rectangle getMBR() {
    return new Rectangle(x, y, x + Math.ulp(x), y + Math.ulp(y));
  }



  public Shape getIntersection(Shape s) {
    return getMBR().getIntersection(s);
  }

  @Override
  public boolean isIntersected(Shape s) {
    return getMBR().isIntersected(s);
  }
  
  @Override
  public String toString() {
    return "Point: ("+x+","+y+")";
  }
  
  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeDouble(x, text, ',');
    TextSerializerHelper.serializeDouble(y, text, '\0');
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    x = TextSerializerHelper.consumeDouble(text, ',');
    y = TextSerializerHelper.consumeDouble(text, '\0');
  }

 @Override
  public int compareTo(Point o) {
   	 if(distance<o.distance) return -1;
	 if(distance>o.distance) return  1;	 
	 return 0;
  }
 

  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
  		int imageHeight, double scale) {
    int imageX = (int) Math.round((this.x - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int imageY = (int) Math.round((this.y - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    g.fillRect(imageX, imageY, 1, 1);  	
  }
  
  @Override
  public void draw(Graphics g, double xscale, double yscale) {
    int imgx = (int) Math.round(x * xscale);
    int imgy = (int) Math.round(y * yscale);
    g.fillRect(imgx, imgy, 1, 1);
  }



}
