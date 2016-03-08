package geospat1.operation1;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import java.awt.Rectangle;
import java.awt.geom.Point2D;

import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;
import com.vividsolutions.jts.geom.Geometry;
//import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateList;
import com.vividsolutions.jts.geom.LinearRing;
public class GeometricUnion{
	public static void main(String args[])
	{
		System.out.println(GeometricUnion.GeometryUnion(args[0],args[1]));
	}
	
    public static boolean GeometryUnion(String input,String output) {
        // TODO Auto-generated method stub
        SparkConf conf=new SparkConf().setAppName("Geometry union").setMaster("spark://10.143.22.47:7077");
        @SuppressWarnings("resource")
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> textfile=sc.textFile(input.trim(),4);
        @SuppressWarnings("serial")
        JavaRDD<Geometry> suni1=textfile.mapPartitions(new FlatMapFunction<Iterator<String>,Geometry> ()
                {
                    public Iterable<Geometry> call(Iterator<String> s)
                    {
                           List<Geometry> h2 = new ArrayList<Geometry>();
				        List<Double> h3=new ArrayList<Double>();
				        List<List<Double>> h4=new ArrayList<List<Double>>();;
						GeometryFactory geom = new GeometryFactory();
						while (s.hasNext()) 
						{
							String p3 =s.next();
							
							 String[] CoordList = p3.split(",");
							 Double x1 = Double.parseDouble(CoordList[2]);
							 Double y1 = Double.parseDouble(CoordList[3]);
							 Double x2 = Double.parseDouble(CoordList[4]);
							 Double y2 = Double.parseDouble(CoordList[5]);
							 h3=new ArrayList<Double>();
							 h3.add(x1);
							 h3.add(y1);
							 h3.add(x2);
							 h3.add(y2);
							 h4.add(h3);						
						}
						h3=null;
						//System.out.println(g4);
						Collections.sort(h4, new Comparator<List<Double>>() {

				            @Override
				            public int compare(List<Double> o1, List<Double> o2) {
				                    // TODO Auto-generated method stub				            	
				                     if((o1.get(0)) <  (o2.get(0)))
				                    	 return -1;
				                     else if((o1.get(0))>(o2.get(0)))
				                    		 return 1;
				                     else
				                    	 return 0;
				            }
						});
						 Double x1 ;
						 Double y1;
						 Double x2 ;
						 Double y2 ;
						for(int i=0;i<h4.size();i++)
						{
							x1=h4.get(i).get(0);
							y1=h4.get(i).get(1);
							x2=h4.get(i).get(2);
							y2=h4.get(i).get(3);
							 Polygon poly = geom.createPolygon(new Coordinate[]{new Coordinate(x1,y1),
									    new Coordinate(x1,y2),
										new Coordinate(x2,y2),
										new Coordinate(x2,y1),
										new Coordinate(x1,y1)});
							h2.add(poly);
							
						}
							
						h4=null;
						
                        Geometry t1=null;
                        List<Geometry> res= new ArrayList<Geometry>();
                        if(!h2.isEmpty())
                        t1=h2.get(0);
                        else
                            t1=null;
                        if(h2.size()==1)
                            res.add(t1);
                        if(!h2.isEmpty())
                        h2.remove(0);
                    
                        int i;
                        for( i=0;!h2.isEmpty();i++)
                        {
                            if(t1.intersects(h2.get(i)) )
                            {
                                t1= t1.union(h2.get(i));
                                h2.remove(i);
                                if(!h2.isEmpty())
                                i=i-1;
                                if(i==h2.size()-1)
                                    i=0;
                                if(h2.isEmpty())
                                {
                                    res.add(t1);
                                    break;
                                }
                                
                            }
                            else if(i==h2.size()-1 && !h2.isEmpty())
                            {
                                
                                res.add(t1);
                                t1=h2.get(0);
                                if(h2.size()==1)
                                    res.add(t1);
                                h2.remove(0);
                                if(h2.isEmpty())
                                    break;
                                
                                if(!h2.isEmpty())
                                i=-1;    
                            }
                        }
			h2=null;
                        return res;
                    }//end of public call
                });//end of map partitions
                    
                
                
            JavaRDD<Geometry> union1=suni1.coalesce(1);
            JavaRDD<Geometry> finalunion=union1.mapPartitions(new FlatMapFunction<Iterator<Geometry>,Geometry>()
                    {
                
                        public Iterable<Geometry> call(Iterator<Geometry> p )
                    
                        {
                            Geometry t2=null;
                            List<Geometry> h3 = new ArrayList<Geometry>();
                            while(p.hasNext())
                            {
                                
                                h3.add(p.next());
                            }
                            List<Geometry> finalun = new ArrayList<Geometry>();
                            if(!h3.isEmpty())
                                t2=h3.get(0);
                                
                                h3.remove(0);
                                if(h3.isEmpty())
                                {
                                finalun.add(t2);
                                return finalun;
                                }
                                int i;
                                for( i=0;!h3.isEmpty();i++)
                                {                                    
                                    if(t2.intersects(h3.get(i)) )
                                    {
                                        t2= t2.union(h3.get(i));
                                        h3.remove(i);
                                        if(!h3.isEmpty())
                                        i=i-1;
                                        if(i==h3.size()-1)
                                            i=0;
                                        if(h3.isEmpty())
                                        {
                                            finalun.add(t2);
                                            break;
                                        }                                    
                                    }
                                    else if(i==h3.size()-1 && !h3.isEmpty())
                                    {
                                        
                                        finalun.add(t2);
                                        t2=h3.get(0);
                                        //System.out.println(t2);
                                        if(h3.size()==1)
                                            finalun.add(t2);
                                        h3.remove(0);
                                        if(h3.isEmpty())
                                            break;
                                        
                                        if(!h3.isEmpty())
                                        i=-1;
                                        
                                        
                                    }
                                }
			h3=null;    
                            return finalun;
                        }
                    
                    });
                    
              //  finalunion.saveAsTextFile("/home/system/Desktop/unionfinal.txt");
                
                JavaRDD<List<Double>> point=finalunion.mapPartitions(new FlatMapFunction<Iterator<Geometry>,List<Double>>()
                        
                        {
                            public Iterable<List<Double>> call(Iterator<Geometry> g)
                            {
                                ArrayList<Coordinate> d=new ArrayList<Coordinate>();
                                List<List<Double>> e=new ArrayList<>();
                                ArrayList<Double> c=new ArrayList<Double>();
                                while(g.hasNext())
                                {
                                    Geometry f=g.next();
                                    Coordinate[] h=f.getCoordinates();
                                    for(int i=0;i<h.length;i++)
                                    {
                                        d=new ArrayList<Coordinate>();
                                        d.add(h[i]);
                                        Double n=d.get(0).getOrdinate(0);
                                        Double n1=d.get(0).getOrdinate(1);
                                        c=new ArrayList<Double>();
                                        c.add(n);
                                        c.add(n1);
                                        e.add(c);
                                    }
                                }
                                e.remove(0);
                                c=null;
				d=null;
				return e;
                                    
                                }
                        });
                
                       
                boolean flag = true;
            	try{
            		point.saveAsTextFile(output.trim());
            	}
            	catch(Exception e)
            	{
            		flag = false;
            	 System.out.println(e.getMessage());
            	}
            	finally
            	{
            		return flag;
            	}
}    
}