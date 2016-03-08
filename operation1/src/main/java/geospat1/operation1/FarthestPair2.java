package geospat1.operation1;



/*
 *
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class FarthestPair2 {
	public static void main(String[] args) {

		//SparkConf conf=new SparkConf().setAppName("Convex").setMaster("spark://10.144.106.88:7077");
			SparkConf conf=new SparkConf().setAppName("FarthestPair").setMaster("spark://10.144.23.24:7077");
			 String  x = args[0];
	    JavaSparkContext sc=new JavaSparkContext(conf);
	    JavaRDD<String> str1=sc.textFile(args[0],4);
	    //"/home/system/Downloads/ProjectTestCase/FarthestPairandClosestPairTestData.csv",4
	   
	   
	    JavaRDD<String> helo = str1.map(new Function<String,String> ()
	            {
	                public String call(String s)
	                {
	                    double num[]=new double[2];
	                    int i=0;
	                    if(s.equalsIgnoreCase("x,y")){return "";}
	                    String[] d=s.split(",");
	                    while(i<2)
	                    {
	                        num[i]=Double.parseDouble(d[i]);
	                        i++;        
	                    }
	                    String u=Arrays.toString(num);
	                    System.out.println(u);
	                    return u; 
	                	
	                	/*
	                	Double num[]=new Double[8];
	                    int i=0;
	                    System.out.println(s+" hello");
	                    String[] d=s.split(",");
	                    while(i<6)
	                    {
	                        
	                        if(i==2)
	                        {
	                        	num[0]=Double.parseDouble(d[i]);
	                        }
	                        else if(i==4)
	                        {
	                        	num[2]=Double.parseDouble(d[i]);
	                            num[6]=Double.parseDouble(d[i]);
	                        }
	                        else if(i==3)
	                        {
	                        	num[1]=Double.parseDouble(d[i]);
	                        }
	                        else if(i==5)
	                        {
	                            num[3]=Double.parseDouble(d[i]);
	                            num[7]=Double.parseDouble(d[i]);
	                        }
	                        i++;        
	                    }
	                    String u=Arrays.toString(num);
	                    return u;*/
	                    
	                }
	            });
	        
	    
	    //sort using X then using Y
	    JavaRDD<Point> XYCoord = helo.mapPartitions(new FlatMapFunction<Iterator<String>,Point> ()
	            {
	    	@Override
	                public Iterable<Point> call(Iterator<String> s)
	                {
	                    ArrayList<String> x = new ArrayList<String>();
	                    ArrayList<String> y = new ArrayList<String>();
	                    List<Point> points = new ArrayList<Point>();
	                    int j = 0;
	                    while (s.hasNext()) {
	                    	String string = s.next();
	                    	if(string.equalsIgnoreCase("")){if(s.hasNext()==true){string=s.next();} else{break;}}
	                    	
	                    x.add(string);
	                    String p2 = x.get(j++);
	                    p2 = p2.substring(1, (p2.length() - 1)).replaceAll(" ","");
	                    String[] p1 = p2.split(",");
	                    double num[] = new double[p1.length];
	                    for (int i = 0; i < p1.length; i++) {

	                    num[i] = Double.valueOf(p1[i]);
	                    num[i+1] = Double.valueOf(p1[i+1]);
	                    points.add(new Point(num[i], num[i+1]));
	                    
	                    i++;
	                    }
	                    }
	                    Collections.sort(points,new Comparator<Point>() {

						public int compare(Point o1, Point o2) {
						    
						    if (o1.getX() == o2.getX()) //If same X, Sort using Y
			                { 
						    	return Double.compare(o1.getY(), o2.getY());
			                    
			                }
						    
							    return Double.compare(o1.getX(), o2.getX());
							}
						});
	                    
	                    
	                    for(int k=0;k<points.size();k++)
	                    {
	                    	System.out.println(points.get(k).getX()+" y: "+points.get(k).getY());
	                    }
	                    return points;
	                    }
	                    }).mapPartitions(new FlatMapFunction<Iterator<Point>,Point>(){

	                		@Override
	                		
	                		public Iterable<Point> call(Iterator<Point> t) throws Exception {
	                			/* Sort S by increasing x and then y-coordinate.
	                			    Let P[] be the sorted array of N points.*/
	                			
	                			List<Point> points = new ArrayList<Point>();
	                			List<Integer> P = new ArrayList<Integer>();
	                			List<Point> ConvexHullPoints = new ArrayList<Point>();
	                			final int MinMin=0;
	                			int MinMax=0,MaxMin=0,MaxMax=0;
	                			while(t.hasNext())
	                			{
	                				points.add(t.next());
	                				t.remove();
	                			}
	                			if(points.size()>0)
	                			{
	                				MaxMax=points.size()-1;
	                			}
	                			//MinMax
	                			Point min = new Point();
	                			int MinInc=0;
	                			min = points.get(MinInc);
	                			double MinX = min.getX();
	                			
	                			while(min.getX()==MinX)
	                			{
	                			MinMax=MinInc;
	                			min = points.get(++MinInc);				
	                			}
	                			System.out.println("MinMax "+ MinMax);
	                			
	                			//MaxMin
	                			Point max = new Point();
	                			int MaxDec=points.size()-1;
	                			max = points.get(MaxDec);
	                			double MaxX = max.getX();
	                			while(max.getX()==MaxX)
	                			{
	                			MaxMin = MaxDec;
	                			max = points.get(--MaxDec);
	                			}
	                			System.out.println("MaxMin "+MaxMin);
	                			//Compute the lower hull stack
	                			Stack<Point> stack = new Stack<Point>();
	                			//Push P[minmin] onto the stack.
	                			stack.push(points.get(MinMin));
	                			for (int i = MinMax+1;i<= MaxMin-1;i++ )
	                			{
	                				// if (P[i] is above or on L_min)
	                				if(isLeft(points.get(MinMin), points.get(MaxMin), points.get(i))>0||isLeft(points.get(MinMin), points.get(MaxMin), points.get(i))==0)
	                				{
	                					continue;
	                				}
	                				while(stack.size()>=2)
	                				{Point PT1= new Point();
	                				 Point PT2= new Point();
	                				 PT1 = stack.peek();
	                				 PT2 = stack.get(stack.indexOf(stack.peek())-1);
	                				 if(isLeft(PT2, PT1, points.get(i))>=0)
	                				 {
	                					 break;
	                				 }
	                				 stack.pop();
	                				}
	                				stack.push(points.get(i));
	                			}
	                			stack.push(points.get(MaxMin)); 
	                			
	                				
	                			// TODO Upper HULL
	                			
	                				if(MaxMax!=MaxMin)
	                					stack.push(points.get(MaxMax));
	                				int bottom=stack.indexOf(stack.peek());
	                				int j=MaxMin;
	                				
	                				while(--j>=MinMax)
	                				{
	                					if(isLeft(points.get(MaxMax),points.get(MinMax),points.get(j))>=0 && j> MinMax)
	                						continue;
	                					while(stack.size()>bottom+1)
	                					{
	                						 Point PT1= new Point();
	                						 Point PT2= new Point();
	                						 PT1 = stack.peek();
	                						 PT2 = stack.get(stack.indexOf(stack.peek())-1);
	                						 if(isLeft(PT2, PT1, points.get(j))>=0)
	                						 {
	                							 break;
	                						 }
	                						 stack.pop();
	                					}
	                					stack.push(points.get(j));
	                				}
	                					if(MinMax!=MinMin)
	                						stack.push(points.get(MinMin));
	                		
	                			
	                			//join lower hull with upper hull and conver this to Iterable dataset.
	                			while(stack.isEmpty()==false)
	                			{System.out.println("Convex Hull points x: "+stack.peek().getX()+" y: "+stack.peek().getY());
	                				ConvexHullPoints.add(stack.pop());
	                			}
	                			
	                			for(int i=0;i<ConvexHullPoints.size();i++)
	                				System.out.println(ConvexHullPoints.get(i));
	                			
	                			return ConvexHullPoints;
	                		}});
	    
	    
	    
	    JavaRDD<Point> convex = XYCoord.coalesce(1).mapPartitions(new FlatMapFunction<Iterator<Point>,Point> ()
	            {
	    	@Override
	                public Iterable<Point> call(Iterator<Point> s)
	                {
	    		System.out.println("2nd Sort");
	                   
	    		List<Point> points=new ArrayList();
	                    int j = 0;
	                    while (s.hasNext()) {
	                       points.add(s.next());
	                                        }
	            
	                    Collections.sort(points,new Comparator<Point>() {

						public int compare(Point o1, Point o2) {
						    
						    if (o1.getX() == o2.getX()) //If same X, Sort using Y
			                { 
						    	return Double.compare(o1.getY(), o2.getY());
			                    
			                }
						    
							    return Double.compare(o1.getX(), o2.getX());
							}
						});
	                    
	                    
	                    for(int k=0;k<points.size();k++)
	                    {
	                    	System.out.println(points.get(k).getX()+" y: "+points.get(k).getY());
	                    }
	                    return points;
	                    }
	                    }).mapPartitions(new FlatMapFunction<Iterator<Point>,Point>(){

	                		@Override
	                		
	                		public Iterable<Point> call(Iterator<Point> t) throws Exception {
	                			/* Sort S by increasing x and then y-coordinate.
	                			    Let P[] be the sorted array of N points.*/
	                			
	                		List<Point> points = new ArrayList<Point>();
	                			List<Integer> P = new ArrayList<Integer>();
	                			List<Point> ConvexHullPoints = new ArrayList<Point>();
	                			final int MinMin=0;
	                			int MinMax=0,MaxMin=0,MaxMax=0;
	                			while(t.hasNext())
	                			{
	                				points.add(t.next());
	                				t.remove();
	                			}
	                			if(points.size()>0)
	                			{
	                				MaxMax=points.size()-1;
	                			}
	                			//MinMax
	                			Point min = new Point();
	                			int MinInc=0;
	                			min = points.get(MinInc);
	                			double MinX = min.getX();
	                			
	                			while(min.getX()==MinX)
	                			{
	                			MinMax=MinInc;
	                			min = points.get(++MinInc);				
	                			}
	                			System.out.println("MinMax "+ MinMax);
	                			
	                			//MaxMin
	                			Point max = new Point();
	                			int MaxDec=points.size()-1;
	                			max = points.get(MaxDec);
	                			double MaxX = max.getX();
	                			while(max.getX()==MaxX)
	                			{
	                			MaxMin = MaxDec;
	                			max = points.get(--MaxDec);
	                			}
	                			System.out.println("MaxMin "+MaxMin);
	                			//Compute the lower hull stack
	                			Stack<Point> stack = new Stack<Point>();
	                			//Push P[minmin] onto the stack.
	                			stack.push(points.get(MinMin));
	                			for (int i = MinMax+1;i<= MaxMin-1;i++ )
	                			{
	                				// if (P[i] is above or on L_min)
	                				if(isLeft(points.get(MinMin), points.get(MaxMin), points.get(i))>0||isLeft(points.get(MinMin), points.get(MaxMin), points.get(i))==0)
	                				{
	                					continue;
	                				}
	                				while(stack.size()>=2)
	                				{Point PT1= new Point();
	                				 Point PT2= new Point();
	                				 PT1 = stack.peek();
	                				 PT2 = stack.get(stack.indexOf(stack.peek())-1);
	                				 if(isLeft(PT2, PT1, points.get(i))>=0)
	                				 {
	                					 break;
	                				 }
	                				 stack.pop();
	                				}
	                				stack.push(points.get(i));
	                			}
	                			stack.push(points.get(MaxMin)); 
	                			
	                				
	                			// TODO Upper HULL
	                			
	                				if(MaxMax!=MaxMin)
	                					stack.push(points.get(MaxMax));
	                				int bottom=stack.indexOf(stack.peek());
	                				
	                				
	                				int j=MaxMin;
	                			
	                				while(--j>=MinMax)

	                				{
	                					if(isLeft(points.get(MaxMax),points.get(MinMax),points.get(j))>=0 && j> MinMax)
	                						continue;
	                					while(stack.size()>bottom+1)
	                					{
	                						 Point PT1= new Point();
	                						 Point PT2= new Point();
	                						 PT1 = stack.peek();
	                						 PT2 = stack.get(stack.indexOf(stack.peek())-1);
	                						 if(isLeft(PT2, PT1, points.get(j))>=0)
	                						 {
	                							 break;
	                						 }
	                						 stack.pop();
	                					}
	                					stack.push(points.get(j));
	                				}
	                					if(MinMax!=MinMin)
	                						stack.push(points.get(MinMin));
	                		
	                			
	                			//join lower hull with upper hull and conver this to Iterable dataset.
	                			while(stack.isEmpty()==false)
	                			{System.out.println("Convex Hull points x: "+stack.peek().getX()+" y: "+stack.peek().getY());
	                				Point stackpoint=stack.pop();
	                				if(!ConvexHullPoints.contains(stackpoint))
	                				  ConvexHullPoints.add(stackpoint);
	                			}
	                			
	                			
	                			return ConvexHullPoints;
	                		}}).coalesce(1);
	    
	    /* To Print the Convex Hull
	     * 
	     * 
	     */

	    JavaRDD<String> dispConvex=convex.mapPartitions(new FlatMapFunction<Iterator<Point>,String>(){
	    	public Iterable<String> call(Iterator<Point> pt)
	    	{
	    		String s="";
	    		ArrayList<String> al=new ArrayList();
	    		while(pt.hasNext())
	    		{
	    			Point p=pt.next();
	    			s=p.getX()+" "+p.getY();
	        		//if(!al.contains(s))
	        			al.add(s);
	    		}
	    		return al;
	    	     
	    	}
	    	
	    	
	    });
	    
    /* To compute the Farthest Pair of points */
    
                JavaRDD<String> farthestPair=convex.mapPartitions(new FlatMapFunction<Iterator<Point>,String>(){
                	   public Iterable<String> call(Iterator<Point> p) throws Exception
                	   {
                		   Point tempPoint;
                		   ArrayList<Point> al=new ArrayList();
                		   ArrayList<String> farpoints=null;
                		   double maxdistance=0;
                		   Iterator<Point> p2=p;
                		   while(p.hasNext())
                		   {
                			   al.add(p.next());
                			   
                		   } 
                			   
                			   for(int i=0;i<al.size()-1;i++)
                			   {
                				   tempPoint=al.get(i);
                				   for(int j=i+1;j<al.size();j++)
                				   {
                					   if(i!=j)
                					   {
                						   Point pt=al.get(j);
                						   double distance=Math.sqrt(((tempPoint.getX()-pt.getX())*(tempPoint.getX()-pt.getX()))+((tempPoint.getY()-pt.getY()))*(tempPoint.getY()-pt.getY()));
                						   System.out.println("Computed distance: "+distance);
                						   if(distance>maxdistance)
                						   {
                							   System.out.println("Max Dist: "+maxdistance+"distance: "+distance);
                							   maxdistance=distance;
                							   farpoints=new ArrayList();
                							   farpoints.add(tempPoint.getX()+","+tempPoint.getY());
                							   farpoints.add(pt.getX()+","+pt.getY());	
                						   
                					   }
                				   }
                			   }
                			   }  
                			   
                			   
                		   
                		   return farpoints;
                	   }
                   }).repartition(1);
              

JavaRDD<String> farthest=farthestPair.mapPartitions(new FlatMapFunction<Iterator<String>,String>(){
	   public Iterable<String> call(Iterator<String> p) throws Exception
	   {
		   String[] tempPoint;
		   ArrayList<String> al=new ArrayList();
		   ArrayList<String> farpoints=null;
		   double maxdistance=0;
		   Iterator<String> p2=p;
		   while(p.hasNext())
		   {
			   al.add(p.next());
			   
		   } 
			   
			   for(int i=0;i<al.size()-1;i++)
			   {
				   tempPoint=al.get(i).split(",");
				   double tx=Double.parseDouble(tempPoint[0]);
				   double ty=Double.parseDouble(tempPoint[1]);
				   for(int j=i+1;j<al.size();j++)
				   {
					   if(i!=j)
					   {
						   String[] pt=al.get(j).split(",");
						   double px=Double.parseDouble(pt[0]);
						   double py=Double.parseDouble(pt[1]);
						   double distance=Math.sqrt(((tx-px)*(tx-px))+((ty-py))*(ty-py));
						   System.out.println("Computed distance: "+distance);
						   if(distance>maxdistance)
						   {
							   System.out.println("Max Dist: "+maxdistance+"distance: "+distance);
							   maxdistance=distance;
							   farpoints=new ArrayList();
							   farpoints.add(tx+","+ty);
							   farpoints.add(px+","+py);	
						   
					   }
				   }
			   }
			   }  
			   
			   
		   
		   return farpoints;
	   }
});

farthest.saveAsTextFile(args[1]);
farthest.foreach(new VoidFunction<String>(){

	@Override
	public void call(String t) throws Exception {
		// TODO Auto-generated method stub
		System.out.println(t);
	}});

	}
	

   
	public static double isLeft( Point P0, Point P1, Point P2 )
	{
	    return (P1.getX() - P0.getX())*(P2.getY() - P0.getY()) - (P2.getX() - P0.getX())*(P1.getY() - P0.getY());
	}
}