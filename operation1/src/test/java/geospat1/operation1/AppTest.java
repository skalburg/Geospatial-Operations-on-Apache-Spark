package geospat1.operation1;

import java.util.Arrays;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
		public static void main(String args[])
		{
	JavaSparkContext spark = new JavaSparkContext(new SparkConf().setAppName("Spark Count").setMaster("local").set("spark.executor.memory", "250m"));
	/*JavaRDD<String> file = spark.textFile("hdfs://hadoopfolder/glutenberg");
	JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
	  public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
	});
	JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
	  public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
	});
	JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer>() {
	  public Integer call(Integer a, Integer b) { return a + b; }
	});
	//counts.saveAsTextFile("hdfs://hadoopfolder/glutenberg/counts.txt");
		System.out.println(counts);*/
	
	JavaRDD<String> lines = spark.textFile("/home/system/IndiaWorldCup");
	 
	JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
		  public Integer call(String s) { return s.length(); }
		});
	
	JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
		  public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
		});
	
	
	// Turn the words into (word, 1) pairs
	JavaPairRDD<String, Integer> ones = words.mapToPair(
	  new PairFunction<String, String, Integer>() {
	    public Tuple2<String, Integer> call(String w) {
	      return new Tuple2<String, Integer>(w, 1);
	    }
	});
	 
	// Group up and add the pairs by key to produce counts
	JavaPairRDD<String, Integer> counts = ones.reduceByKey(
	  new Function2<Integer, Integer, Integer>() {
	    public Integer call(Integer i1, Integer i2) {
	      return i1 + i2;
	    }
	});
	 
	counts.saveAsTextFile("/home/system/output.txt");
		
		}
}
