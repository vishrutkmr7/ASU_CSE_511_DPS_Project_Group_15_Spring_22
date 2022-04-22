# CSE 511: DPS Project for Group 15, Spring '22

Project files for ASU's course CSE 511: Data Processing at Scale, Group 15, for the Spring 2022 semester

## Members

- [Vishrut Jha](mailto:vkjha@asu.edu)
- [Sagar Cariappa](mailto:skuppand@asu.edu)
- [Ravi Maddi](mailto:rmaddi1@asu.edu)
- [Aniruddha Mondal](mailto:amondal8@asu.edu)

### Project description

#### Phase 1

A major peer-to-peer taxi cab firm has hired your team to develop and run multiple spatial queries on their large database that contains geographic data as well as real-time location data of their customers. A spatial query is a special type of query supported by geo-databases and spatial databases. The queries differ from traditional SQL queries in that they allow for the use of points, lines, and polygons. The spatial queries also consider the relationship between these geometries. Since the database is large and mostly unstructured, your client wants you to use a popular Big Data software application, SparkSQL. The goal of the project is to extract data from this database that will be used by your client for operational (day-to-day) and strategic level (long term) decisions.

In the first phase, you will write two user-defined functions `ST_Contains` and `ST_Within` in SparkSQL and use them to run the following four spatial queries. Here, a rectangle R represents a geographical boundary in a town or city, and a set of points P represents customers who request taxi cab service using your client firm’s app.

1. Range query: Given a query rectangle R and a set of points P, find all the points within R. You need to use the `ST_Contains` function in this query.

2. Range join query: Given a set of rectangles R and a set of points P, find all (point, rectangle) pairs such that the point is within the rectangle.

3. Distance query: Given a fixed point location P and distance D (in kilometers), find all points that lie within a distance D from P. You need to use the `ST_Within` function in this query.

4. Distance join query: Given two sets of points P1 and P2, and a distance D (in kilometers), find all (p1, p2) pairs such that p1 is within a distance D from p2 (i.e., p1 belongs to P1 and p2 belongs to P2). You need to use the `ST_Within` function in this query.

The full instruction is here: Project Milestone 2: Introduction to Course Project

The coding template is [here](https://github.com/jiayuasu/CSE512-Project-Phase2-Template).

##### Submission Guidance

You will submit two files as follows:

1. One zip file. Please compress your Scala project to a single ZIP file and submit it in this Phase. Note that: you need to make sure your project can compile by entering `sbt assembly` in the terminal.

2. One jar file. The jar file should be able to run using `spark submit`.

#### Phase 2

In this project, you are required to do spatial hot spot analysis. In particular, you need to complete two different hot spot analysis tasks.

1. Hot zone analysis
This task will need to perform a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle will be obtained. The hotter rectangle means that it includes more points. So this task is to calculate the hotness of all the rectangles.

2. Hot cell analysis
This task will focus on applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark. The topic of this task is from ACM SIGSPATIAL GISCUP 2016.

The Problem Definition page is [here](http://sigspatial2016.sigspatial.org/giscup2016/problem).

The Submit Format page is [here](http://sigspatial2016.sigspatial.org/giscup2016/submit).

Special requirement (different from GIS CUP)

As stated in the Problem Definition page, in this task, you are asked to implement a Spark program to calculate the Getis-Ord statistic of NYC Taxi Trip datasets. We call it "Hot cell analysis"

To reduce the computation power need, we made the following changes:

The input will be a monthly taxi trip dataset from 2009 - 2012. For example, `yellow_tripdata_2009-01_point.csv`, `yellow_tripdata_2010-02_point.csv`.

Each cell unit size is 0.01 * 0.01 in terms of latitude and longitude degrees.

You should use 1 day as the Time Step size. The first day of a month is step 1. Every month has 31 days.

You only need to consider Pick-up Location.

We don't use Jaccard similarity to check your answer. However, you don't need to worry about how to decide the cell coordinates because the code template generated cell coordinates. You just need to write the rest of the task.

##### Coding template specification

###### Input parameters

Output path (Mandatory)

Task name: `hotzoneanalysis` or `hotcellanalysis`

Task parameters: (1) Hot zone (2 parameters): nyc taxi data path, zone path(2) Hot cell (1 parameter): nyc taxi data path

Example:

    test/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow_tripdata_2009-01_point.csv

Note:

The number/order of tasks do not matter.

But, the first 7 of our final test cases will be hot zone analysis, the last 8 will be hot cell analysis.

###### Input data format

The main function/entrance is `cse512.Entrance` scala file.

1. Point data: the input point dataset is the pickup point of New York Taxi trip datasets. The data format of this phase is the original format of NYC taxi trip which is different from `Phase 2`. But the coding template already parsed it for you. Find the data in the .zip file below.

`yellow_tripdata_2009-01_point.csv`
2. Zone data (only for hot zone analysis): at "src/resources/zone-hotzone" of the template

Hot zone analysis

The input point data can be any small subset of NYC taxi dataset.

Hot cell analysis

The input point data is a monthly NYC taxi trip dataset (2009-2012) like `yellow_tripdata_2009-01_point.csv`

###### Output data format

Hot zone analysis

All zones with their count, sorted by "rectangle" string in an ascending order.

Hot cell analysis

The coordinates of top 50 hottest cells sorted by their G score in a descending order. Note, DO NOT OUTPUT G score.

    -7399,4075,15
    -7399,4075,29
    -7399,4075,22

###### Example answers

An example input and answer are put in `testcase` folder of the coding template

##### Where you need to change

DO NOT DELETE any existing code in the coding template unless you see this "YOU NEED TO CHANGE THIS PART"

###### Hot zone analysis

In the code template,

You need to change `HotzoneAnalysis.scala` and `HotzoneUtils.scala`.

The coding template has loaded the data and wrote the first step, range join query, for you. Please finish the rest of the task.

The output DataFrame should be sorted by you according to "rectangle" string.

###### Hot cell analysis

In the code template,

You need to change `HotcellAnalysis.scala` and `HotcellUtils.scala`.

The coding template has loaded the data and decided the cell coordinate, x, y, z and their min and max. Please finish the rest of the task.

The output DataFrame should be sorted by you according to G-score. The coding template will take the first 50 to output. DO NOT OUTPUT G-score.

##### Submission

###### Submission files

Submit your project jar package in My Submission Tab.

Note: You need to make sure your code can compile and package by entering sbt clean assembly. We will run the compiled package on our cluster directly using "spark-submit" with parameters. If your code cannot compile and package, you will not receive any points.

##### Tips (Optional)

###### How to debug your code in IDE

If you are using the Scala template,

1. Use IntelliJ Idea with Scala plug-in or any other Scala IDE.

2. Replace the logic of User Defined Functions ST_Contains and ST_Within in SpatialQuery.scala.

3. Append `.master("local[*]")` after `.config("spark.some.config.option", "some-value")` to tell IDE the master IP is localhost.

4. In some cases, you may need to go to `build.sbt` file and change `% provided` to `% compile` in order to debug your code in IDE

5. Run your code in IDE

6. You must revert Step 3 and 4 above and recompile your code before use of spark-submit!!!

###### How to submit your code to Spark

If you are using the Scala template

1. Go to project root folder,

2. Run sbt clean assembly. You may need to install sbt in order to run this command.

3. Find the packaged jar in "./target/scala-2.11/CSE511-Project-Hotspot-Analysis-Template-assembly-0.1.0.jar"

4 .Submit the jar to Spark using Spark command `./bin/spark-submit`. A pseudo code example:

    ./bin/spark-submit ~/GitHub/CSE511-Project-Hotspot-Analysis-Template/target/scala-2.11/CSE511-Project-Hotspot-Analysis-Template-assembly-0.1.0.jar test/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow_tripdata_2009-01_point.csv
