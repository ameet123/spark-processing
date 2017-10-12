### Spark Processing with Scala

This project describes various ways in which we can perform spark processing.
All the processing is to be invoked using `spark-shell`

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
