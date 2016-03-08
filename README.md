# Geospatial-Operations-on-Apache-Spark
Demonstration of big data geospatial operations on Apache Spark

Project group members:

1) Rakesh Subramanian Suresh 
2) Arpan Chatterjee 
3) Siddharth Mohan
4) Kranthi Sai Davuluri
5) Sagar Kalburgi


To run the file we need .jar:
for Creating jar. Import the project using eclipse and create jar as follows
Righ Click on project file -> Run as ->Maven install
The jar will be located in the workspace. Place the .jar file in spark folder.

To Create HDFS Path

1)Creating hadoop input file, arguments, structure of command
Place arealm and areawater files in Hadoop file system
1.1)First go to hadoop/bin folder
Creating directory
hadoop dfs -mkdir /content
1.2)load file from local to hdfs
hadoop dfs -copyFromLocal /<File path> /content



 Modification to be done in code
-----------------------------------

1) In the code, on the line declaring JavaSparkContext, change the argument of setMaster function from local to spark://<Master IP address>:7077

2) Comment/Uncomment appropriate lines indicated in source code, depending on running the code on test dataset or big dataset.
    Inorder to run the bigdata set, some lines in the code that have been commented need to be uncommented.

3) To run the program go to the spark bin folder and execute the following commands


TO RUN THE PROJECT ON SPARK USING TERMINAL
--------------------------------------------


Changes to be made 
- Master Address
- Additional Jar file location ( for library jar and Project jar) 
- Input/Output path

Convex Hull
---------------------

./bin/spark-submit --class geospat1.operation1.ConvexHull  --master spark://10.144.23.24:7077 operation1-0.0.1-SNAPSHOT.jar hdfs://master:54310/inputfiles/ProjectTestCase/ConvexHullTestData.csv hdfs://master:54310/outputfiles/Convexhull




Geometry Closest Pair
----------------------
./bin/spark-submit --class geospat1.operation1.GeometryClosestPair --master spark://10.144.23.24:7077 operation1-0.0.1-SNAPSHOT.jar hdfs://master:54310/inputfiles/ProjectTestCase/FarthestPairandClosestPairTestData.csv hdfs://master:54310/outputfiles/Closest


FarthestPair
--------------

./bin/spark-submit --class geospat1.operation1.FarthestPair --master spark://10.144.23.24:7077 operation1-0.0.1-SNAPSHOT.jar hdfs://master:54310/inputfiles/ProjectTestCase/FarthestPairandClosestPairTestData.csv hdfs://master:54310/outputfiles/Convexhull



Geometric Union
-----------------

./bin/spark-submit --class geospat1.operation1.GeometricUnion --jars jts-1.13.jar --master spark://10.144.23.24:7077 operation1-0.0.1-SNAPSHOT.jar hdfs://master:54310/inputfiles/ProjectTestCase/PolygonUnionTestData.csv hdfs://master:54310/outputfiles/Union2



Join Query
--------------------
./bin/spark-submit --class geospat1.operation1.SpatialJoin --master spark://10.144.23.24:7077 operation1-0.0.1-SNAPSHOT.jar hdfs://master:54310/inputfiles/ProjectTestCase/JoinQueryBigRectangles.csv hdfs://master:54310/inputfiles/ProjectTestCase/JoinQuerySmallRectangles.csv hdfs://master:54310/outputfiles/SpatialJoin1


Range Query
------------

./bin/spark-submit --class geospat1.operation1.SpatialRange --master spark://10.144.23.24:7077 operation1-0.0.1-SNAPSHOT.jar hdfs://master:54310/inputfiles/ProjectTestCase/RangeQueryBigRectangles.csv hdfs://master:54310/inputfiles/ProjectTestCase/RangeQuerySmallRectangles.csv hdfs://master:54310/outputfiles/SpatialRange

Heat Map
------------
./bin/spark-submit --class geospat1.operation1.HeatMap --master spark://10.144.23.24:7077 operation1-0.0.1-SNAPSHOT.jar hdfs://master:54310/inputfiles/ProjectTestCase/JoinQueryBigRectangles.csv hdfs://master:54310/inputfiles/ProjectTestCase/JoinQuerySmallRectangles.csv hdfs://master:54310/outputfiles/HeatMap


For Convex Hull:

Convex hull needs one input file path and one output file path following the "spark-submit" command.
Reminder: Change the master ip address in the code before executing.


For ClosestPair:

ClosestPair needs one input file path and one output file path following the "spark-submit" command.
Reminder: Change the master ip address in the code before executing.

For FarthestPair:

FarthestPair needs one input file path and one output file path following the "spark-submit" command.
Reminder: Change the master ip address in the code before executing.

For Spatial join:


./spark-submit --class <package>.SpatialJoin --master spark://<masterIp>:7077 <Input_file1_Path> <Input_file2_Path> <Output_FilePath>

where <Input_file1_Path> is the HDFS pathname for Areawater, <Input_file2_Path> is the HDFS pathname for arealm and <Output_FilePath> is the HDFS pathname for output file. (Please note that if we are running the code using test data, two input files need to be mentioned, one input file containing only target rectangles and another input file containing only query rectangles).


For Heat Map:


./spark-submit --class <package>.HeatMap --master spark://<masterIp>:7077 <Input_file1_Path> <Input_file2_Path> <Output_FilePath>

where <Input_file1_Path> is the HDFS pathname for Bigger Rectangles, <Input_file2_Path> is the HDFS pathname for arealm and <Output_FilePath> is the HDFS pathname for output file. (Please note that if we are running the code using test data, two input files need to be mentioned, one input file containing only target rectangles and another input file containing only query rectangles).

4) The output file on <Output_FilePath> will have the output in the following form:
x1,y1,x2,y2 contains 
x3,y3,x4,y4
x5,y5,x6,y6
.......
......

where each line corresponds to coordinates of a rectangle. Here rectangle with coordinates x1,y1,x2,y2 corresponds to an entry in areawater dataset, and contains all the rectangles in the subsequent lines(these are from arealm dataset). If a rectangle from areawater dataset does not contain any rectangle from arealm, then it will not be followed by the same. The above pattern is formed for each rectangle from Areawater dataset.

Please note: Test data sets for Spatial join have been provided with the submission JoinQueryBigRectangles.csv and JoinQuerySmallRectangles.csv.


For Range Query:


./spark-submit --class <package>.RangeQuery spark://<masterIp>:7077 <Input_file1_Path> <Input_file2_Path> <Output_FilePath>

where <Input_file1_Path> is the HDFS pathname for Areawater/AreaLM, <Input_file2_Path> is the HDFS pathname for RangeQueryBigRectangles.csv File (provided with the submission) and <Output_FilePath> is the HDFS pathname for output file. (Please note that if we are running the code using test data, two input files need to be mentioned, one input file containing only target rectangles and another input file containing only query rectangles).
5) The output file on <Output_FilePath> will have the output in the following form:
x1,y1,x2,y2 contains 
x3,y3,x4,y4
x5,y5,x6,y6
.......
......

where each line corresponds to coordinates of a rectangle. Here rectangle with coordinates x1,y1,x2,y2 corresponds to an entry in areawater dataset, and contains all the rectangles in the subsequent lines(these are from arealm dataset). If a rectangle from areawater dataset does not contain any rectangle from arealm, then it will not be followed by the same. The above pattern is formed for each rectangle from Areawater dataset.


Geometry Union:
--------------

Geometric Union
1)Set dependency for jts 1.13 in maven
    <dependency>
    	<groupId>com.vividsolutions</groupId>
    	<artifactId>jts</artifactId>
    	<version>1.13</version>
    </dependency>
2) download JTS jar and put  inside spark folder
3)Change master ip accordingly in
static SparkConf conf=new SparkConf().setAppName("Geometry union").setMaster("spark://10.144.83.64");
4)For testing on arealm dataset just comment these 4 lines that takes coordinate values and uncomment the specified lines as in .java file
                                                                        Double x1 = Double.parseDouble(CoordList[0]);                        
                                                                        Double y1 = Double.parseDouble(CoordList[1]);
                                                                        Double x2 = Double.parseDouble(CoordList[2]);
                                                                        Double y2 = Double.parseDouble(CoordList[3]);
5)Running spark command:Go to /Spark/bin folder. Important specify jar download path--jars <>

./spark-submit --class <package>.GeometricUnion --jars <JTS download path> --master spark://<masterIp>:7077 <ProjectjarPath> <Input_file_Path> <Output_FilePath>


