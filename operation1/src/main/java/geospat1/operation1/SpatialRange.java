package geospat1.operation1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Function1;
import scala.Tuple2;

public class SpatialRange {

	/**
	 * @param args
	 */
	static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Range").setMaster("spark://10.143.22.47:7077"));
	public static void main(String[] args) throws IOException, URISyntaxException 
	{
		System.out.println(RangeQuery(args[0],args[1],args[2]));
	
	}
	
	static Boolean RangeQuery(String InputLocation1, String InputLocation2, String OutputLocation) throws IOException, URISyntaxException
	{
		JavaRDD<String> str1=sc.textFile(InputLocation2,4);
		List<Double> big=new ArrayList<Double>();
		List<Double> window=new ArrayList<Double>();
		
		//Read the file that contains the query rectangle (Provided with submission)
		Path pt=new Path(InputLocation1);
        FileSystem fs = FileSystem.get(new URI(InputLocation1.trim()),new Configuration());
       // BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		
		try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	//If the line in the text file contains text, skip it
		    	if(line.contains("Target") || line.contains("x1") || line.contains(",,,"))
		    		continue;
		    	String[] w=line.split(",");	    	
				big.add(Double.parseDouble(w[2]));
				big.add(Double.parseDouble(w[3]));
				big.add(Double.parseDouble(w[4]));
				big.add(Double.parseDouble(w[5]));
		    }
		    //The coordinates of the bigger rectangle is extracted here
		    window.add(big.get(big.size()-4));
		    window.add(big.get(big.size()-3));
		    window.add(big.get(big.size()-2));
		    window.add(big.get(big.size()-1));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		//Declare a broadcast variable to store bigger rectangles as ArrayList
		final Broadcast<List<Double>> win=sc.broadcast(window);	
		JavaRDD<String> output=str1.mapPartitions(new FlatMapFunction<Iterator<String>,String> ()
				{
					public Iterable<String> call(Iterator<String> s)
					{
						double num[]=new double[4];
						List<String> result=new ArrayList<String>();
						int k=0,l=0;
						//List<Double> bigger=new ArrayList<Double>();
						List<Double> smaller=new ArrayList<Double>();
						
						//Reading smaller rectangles from Arealm file
						/*
						 * To Run the big data set
						 */
						
						while(s.hasNext())
						{
							String d=s.next();
							String[] w=d.split(",");
								smaller.add(Double.parseDouble(w[2]));
								smaller.add(Double.parseDouble(w[3]));
								smaller.add(Double.parseDouble(w[4]));
								smaller.add(Double.parseDouble(w[5]));
						} 
						
						//If you want to test this code for test data set, comment the above try/catch block and uncomment
					    //the following try/catch block
						/*
						
						while(s.hasNext())
						{
							String d=s.next();
							if(d.contains("Query") || d.contains("x1")|| d.contains(",,,"))
					    		continue;
							String[] w=d.split(",");
							//Extract all the coordinates of smaller rectangles
							    smaller.add(Double.parseDouble(w[0]));
								smaller.add(Double.parseDouble(w[1]));
								smaller.add(Double.parseDouble(w[2]));
								smaller.add(Double.parseDouble(w[3]));										
						}*/
							
		
						
						//Extract bigger rectangles from broadcast variable
							List<Double> bigger=win.value();
							
							//Loop coordinates of each smaller rectangle for the query rectangle, add it to result if the smaller rectangle is inside the query rectangle
							
							for(int i=0;i<bigger.size()-3;i+=4)
							{
								result.add("("+bigger.get(i)+","+bigger.get(i+1)+","+bigger.get(i+2)+","+bigger.get(i+3)+")"+"contains");
								for(int j=0;j<smaller.size()-7;j+=4)
								{			
									if(bigger.get(i)<smaller.get(j) && bigger.get(i+2)>smaller.get(j) && 
									bigger.get(i+1)<smaller.get(j+1) && bigger.get(i+3)>smaller.get(j+1) &&
									bigger.get(i+2)>smaller.get(j+2) && bigger.get(i)<smaller.get(j+2) &&
									bigger.get(i+3) > smaller.get(j+3) && bigger.get(i+1)<smaller.get(j+3))
									{
										result.add(smaller.get(j)+","+smaller.get(j+1)+","+smaller.get(j+2)+","+smaller.get(j+3));
									}
									}
							}		
							
						return result;
					}				
				});
			JavaRDD<String> abc=output.coalesce(1);
			try
			{
			JavaRDD<String> finale=abc.mapPartitions(new FlatMapFunction<Iterator<String>,String> ()
					{
						public Iterable<String> call(Iterator<String> s)
						{
							double num[]=new double[4];
							List<String> result=new ArrayList<String>();
							List<Double> smaller=new ArrayList<Double>();
							while(s.hasNext())
							{
								String current=s.next();
								if(current.contains("contains"))
									continue;				
								//Remove lines containing coordinates for bigger rectangles
								String d=current;
								String[] w=d.split(",");
									smaller.add(Double.parseDouble(w[0]));
									smaller.add(Double.parseDouble(w[1]));
									smaller.add(Double.parseDouble(w[2]));
									smaller.add(Double.parseDouble(w[3]));
							}
			
								List<Double> biggest=win.value();
								
								//Remove the redundant coordinate entries for smaller rectangles
								for(int i=0;i<biggest.size()-3;i+=4)
								{
									result.add("("+biggest.get(i)+","+biggest.get(i+1)+","+biggest.get(i+2)+","+biggest.get(i+3)+")"+"contains");
									for(int j=0;j<smaller.size()-3;j+=4)
									{			
										if(biggest.get(i)<smaller.get(j) && biggest.get(i+2)>smaller.get(j) && 
												biggest.get(i+1)<smaller.get(j+1) && biggest.get(i+3)>smaller.get(j+1) &&
												biggest.get(i+2)>smaller.get(j+2) && biggest.get(i)<smaller.get(j+2) &&
												biggest.get(i+3) > smaller.get(j+3) && biggest.get(i+1)<smaller.get(j+3))
										{
											result.add("("+smaller.get(j)+","+smaller.get(j+1)+"),("+smaller.get(j+2)+","+smaller.get(j+3)+")");
										}
										}
								}		
								return result;				
						}
						
				} 
			);
			finale.saveAsTextFile(OutputLocation);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				return false;
			}
			//Return true if the query rectangle contains smaller rectangles	
			return true;
			}
	}