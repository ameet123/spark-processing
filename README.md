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
