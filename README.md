### Spark Processing with Scala

This project describes various ways in which we can perform spark processing.
All the processing is to be invoked using `spark-shell`

Features:

+ Read input array from command line to spark-shell via `--conf spark.driver.args="in1 in2"`
+ `CSV` datafile is assumed to not contain headers or schema.
+ schema read from a matching metadata file from the same directory. Its name can be parameterized if desired.
+ Metadata file contains comma separated list of field names
+ This schema is attached to the data files from that directory.
+ A single monolithic column generated with specified delimiter.
+ The data can be eventually written to HDFS with a single reducer to generate a single file.s
+ The file can be in desired format such as - `ORC, Parquet`.
+ all files are read together into a single dataframe and processed in one shot for efficiency.

#### 1. Script
The script *spark-script.scala* can be loaded as is and run inside spark-shell.
It contains actual processing commands without any organization or arrangement using
functions.

#### 2. Object
This is an object of Scala called IIDRSpark and can be invoked by,

```scala
IIDRSpark.main(inputArray)
```

#### 3. Scala Class

Here we organize processing into class and methods for efficient processing and better readability.

```scala
val iidr = new IIDRSparkProcessing(sc = sc, spark = spark, inputDirs = inputArray)
```

### Sample Execution

```scala
ameet@ubuntu:~/Documents/anthem$ spark-shell -i 
class.spark                IIDRSparkProcessing.spark  metastore_db/              sample.csv
.class.spark.swp           in1/                       new.spark                  test_no.csv
csvPreToPost.py            in2/                       object.spark               test.py
derby.log                  meta.dat                   out/                       test.spark
ameet@ubuntu:~/Documents/anthem$ spark-shell -i IIDRSparkProcessing.spark --conf spark.driver.args="in1 in2"
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
17/10/12 11:57:42 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
17/10/12 11:57:43 WARN Utils: Your hostname, ubuntu resolves to a loopback address: 127.0.1.1; using 192.168.255.130 instead (on interface ens33)
17/10/12 11:57:43 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
17/10/12 11:57:49 WARN ObjectStore: Failed to get database global_temp, returning NoSuchObjectException
Spark context Web UI available at http://192.168.255.130:4040
Spark context available as 'sc' (master = local[*], app id = local-1507834663901).
Spark session available as 'spark'.
Loading IIDRSparkProcessing.spark...
defined class IIDRSparkProcessing

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.2.0
      /_/
         
Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_144)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val iidr = new IIDRSparkProcessing(sc, spark, Array("in1","in2"))
iidr: IIDRSparkProcessing = IIDRSparkProcessing@2ca57c02

scala> iidr.run
root
 |-- monolith: string (nullable = false)

+---------------------------------------------------------------+
|monolith                                                       |
+---------------------------------------------------------------+
|id|name|state| country| degree| college| rd| house|test2_91.csv|
|3|rafa| GA| US| BS| UMich| hugg rd| 1234|test2_91.csv          |
|4|fedex| MD| US| MS| UChicago| phew rd| 344|test2_91.csv       |
|id|name|state| country| degree| college| rd| house|test2_89.csv|
|3|rafa| GA| US| BS| UMich| hugg rd| 1234|test2_89.csv          |
|4|fedex| MD| US| MS| UChicago| phew rd| 344|test2_89.csv       |
|id|name|state| country| degree| college| rd| house|test2_57.csv|
|3|rafa| GA| US| BS| UMich| hugg rd| 1234|test2_57.csv          |
|4|fedex| MD| US| MS| UChicago| phew rd| 344|test2_57.csv       |
|id|name|state| country| degree| college| rd| house|test2_81.csv|
|3|rafa| GA| US| BS| UMich| hugg rd| 1234|test2_81.csv          |
|4|fedex| MD| US| MS| UChicago| phew rd| 344|test2_81.csv       |
|id|name|state| country| degree| college| rd| house|test2_92.csv|
|3|rafa| GA| US| BS| UMich| hugg rd| 1234|test2_92.csv          |
|4|fedex| MD| US| MS| UChicago| phew rd| 344|test2_92.csv       |
|id|name|state| country| degree| college| rd| house|test2_94.csv|
|3|rafa| GA| US| BS| UMich| hugg rd| 1234|test2_94.csv          |
|4|fedex| MD| US| MS| UChicago| phew rd| 344|test2_94.csv       |
|id|name|state| country| degree| college| rd| house|test2_60.csv|
|3|rafa| GA| US| BS| UMich| hugg rd| 1234|test2_60.csv          |
+---------------------------------------------------------------+
only showing top 20 rows

```