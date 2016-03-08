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

public class SpatialJoin {

	static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spatial").setMaster("spark://10.143.22.47:7077"));
	public static void main(String[] args) throws IOException, URISyntaxException 
	{
		System.out.println(SpatialJoinQuery(args[0],args[1],args[2]));
	}
	
	static Boolean SpatialJoinQuery(String InputLocation1, String InputLocation2, String OutputLocation) throws IOException, URISyntaxException
	{
			
			JavaRDD<String> str1=sc.textFile(InputLocation2.trim(),4);
			List<Double> big=new ArrayList<Double>();
			
			//Read file that contains Areawater dataset 
			Path pt=new Path(InputLocation1);
            FileSystem fs = FileSystem.get(new URI(InputLocation1.trim()),new Configuration());
           // BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            
            /* To Run the big dataset */
           
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)))) {
			    String line;
			    while ((line = br.readLine()) != null) {
			    	String[] w=line.split(",");
					big.add(Double.parseDouble(w[2]));
					big.add(Double.parseDouble(w[3]));
					big.add(Double.parseDouble(w[4]));
					big.add(Double.parseDouble(w[5]));
			    }
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
            
            
            //If you want to test this code for test data set, comment the above try/catch block and uncomment
            //the following try/catch block
            
			/*
			try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)))) {
			    String line;
			    while ((line = br.readLine()) != null) {
			    	if(line.contains("Target") || line.contains("x1"))
			    		continue;
			    	String[] w=line.split(",");
					big.add(Double.parseDouble(w[0]));
					big.add(Double.parseDouble(w[1]));
					big.add(Double.parseDouble(w[2]));
					big.add(Double.parseDouble(w[3]));
			    }
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			*/
            
            //Creating broadcast variable for ArrayList that has bigger rectangles
			final Broadcast<List<Double>> bigger=sc.broadcast(big);		
			JavaRDD<String> output=str1.mapPartitions(new FlatMapFunction<Iterator<String>,String> ()
					{
						public Iterable<String> call(Iterator<String> s)
						{
							double num[]=new double[4];
							List<String> result=new ArrayList<String>();
							List<Double> smaller=new ArrayList<Double>();
			//Reading smaller rectangles from Arealm file			
							
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
							
							/* while(s.hasNext())
							{
								String d=s.next();
								if(d.contains("Query") || d.contains("x1"))
						    		continue;
								String[] w=d.split(",");
									smaller.add(Double.parseDouble(w[0]));
									smaller.add(Double.parseDouble(w[1]));
									smaller.add(Double.parseDouble(w[2]));
									smaller.add(Double.parseDouble(w[3]));
							} */
							
					//For each big rectangle, find the smaller rectangles it contains
								List<Double> biggest=bigger.value();
								for(int i=0;i<biggest.size()-3;i+=4)
								{
									result.add(biggest.get(i)+","+biggest.get(i+1)+","+biggest.get(i+2)+","+biggest.get(i+3)+"contains");
									for(int j=0;j<smaller.size()-3;j+=4)
									{			
										
										if(biggest.get(i)<smaller.get(j) && biggest.get(i+2)>smaller.get(j) && 
										biggest.get(i+1)<smaller.get(j+1) && biggest.get(i+3)>smaller.get(j+1) &&
										biggest.get(i+2)>smaller.get(j+2) && biggest.get(i)<smaller.get(j+2) &&
										biggest.get(i+3) > smaller.get(j+3) && biggest.get(i+1)<smaller.get(j+3))
										{							
											result.add(smaller.get(j)+","+smaller.get(j+1)+","+smaller.get(j+2)+","+smaller.get(j+3));
										}
									}
								}		
								
							return result;
						}				
					});
			JavaRDD<String> abc=output.coalesce(1);
    //Combine the outputs from each task to single output
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
							String d=current;
							String[] w=d.split(",");
								smaller.add(Double.parseDouble(w[0]));
								smaller.add(Double.parseDouble(w[1]));
								smaller.add(Double.parseDouble(w[2]));
								smaller.add(Double.parseDouble(w[3]));
						}
						
						ArrayList<Double> tiny=new ArrayList<Double>();		
						int flag=0;
						tiny.add(smaller.get(0));
						tiny.add(smaller.get(1));
						tiny.add(smaller.get(2));
						tiny.add(smaller.get(3));
			//Removing redundant entries
						for(int k=4;k<smaller.size()-3;k+=4)
						{					
							flag=1;
							for(int j=0;j<tiny.size()-3;j+=4)
							{
								if(smaller.get(k).equals(tiny.get(j)) && smaller.get(k+1).equals(tiny.get(j+1)) && smaller.get(k+2).equals(tiny.get(j+2)) && smaller.get(k+3).equals(tiny.get(j+3)))
								{
									flag++;
								}
							}
							if(flag==1)
							{
							tiny.add(smaller.get(k));
							tiny.add(smaller.get(k+1));
							tiny.add(smaller.get(k+2));
							tiny.add(smaller.get(k+3));
							}
						}
							List<Double> biggest=bigger.value();
							for(int i=0;i<biggest.size()-3;i+=4)
							{
								result.add(biggest.get(i)+","+biggest.get(i+1)+","+biggest.get(i+2)+","+biggest.get(i+3));
					
								for(int j=0;j<tiny.size()-3;j+=4)
								{			
									if(biggest.get(i)<tiny.get(j) && biggest.get(i+2)>tiny.get(j) && 
									biggest.get(i+1)<tiny.get(j+1) && biggest.get(i+3)>tiny.get(j+1) &&
									biggest.get(i+2)>tiny.get(j+2) && biggest.get(i)<tiny.get(j+2) &&
									biggest.get(i+3) > tiny.get(j+3) && biggest.get(i+1)<tiny.get(j+3))
									{	
										result.add(tiny.get(j)+","+tiny.get(j+1)+","+tiny.get(j+2)+","+tiny.get(j+3));								
									}
									}
							}		
							return result;				
					}
					
			} 
		);
	
		finale.saveAsTextFile(OutputLocation.trim());
		}
		
		catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}
		
	}
	