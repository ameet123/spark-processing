import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by ac on 10/12/2017.
  */
object IIDRSpark extends App {
  // Constants and variables
  val METADATA_END_INDEX = 1
  val METADATA_FILE_NAME = "meta.dat"
  val DATA_FORMAT = "csv"

  val sc = new SparkContext()
  val spark = new SparkSession()

  // *** Functions
  /**
    * based on the command line args, generate a schema structure
    *
    * @param metaFile
    * @return
    */
  def getSchema(metaFile: String): StructType = {
    val metaDF = spark.read.format(DATA_FORMAT).option("header", "false").load("meta.dat")
    val metaSchema = StructType(
      metaDF.first.toSeq.toList.map
      (i => i.asInstanceOf[String].trim).map(col => StructField(col, StringType))
    )
    metaSchema
  }

  /**
    * create an array of desired column indexes
    * we need [Int] otherwise we will get polymorphic exception
    * val ind = ((0 to 1).toArray) ++ (((csvDF.columns.length-2)/2 to csvDF.columns.length-1).toArray[Int])
    *
    * @param myDF
    * @return
    */
  def createColIndexArray(myDF: DataFrame): Array[Int] = {
    val ind1 = (0 to METADATA_END_INDEX).toArray
    val ind2 = ((myDF.columns.length - 2) / 2 until myDF.columns.length).toArray
    ind1 ++ ind2
  }

  /**
    * write into a single file via a single reducer to HDFS
    *
    * @param outDir
    * @param df
    */
  def writeToHdfs(outDir: String, df: DataFrame): Unit = {
    df.repartition(1).write.text(outDir)
  }

  /**
    * extract file name from abs path
    *
    * @return
    */
  def getMatchingFileName: (String => String) = { s => {
    val f = new File(s)
    f.getName
  }
  }

  /**
    * a UDF to extract file name from abs path
    */
  val udfFileName = udf(getMatchingFileName)

  // **** END of functions

  // ** Execution
  // get command line arguments passed a space separated list of dirs
  val listOfInputDirs = sc.getConf.get("spark.driver.args").split("\\s+")

  // option("inferSchema", "true") option("header","true").
  val csvDF = spark.read.format(DATA_FORMAT).
    option("delimiter", ",").
    schema(getSchema(METADATA_FILE_NAME)).
    load(listOfInputDirs: _*)

  val ind = createColIndexArray(csvDF)

  // get matching columns from DF
  // csvDF.columns => get all columns from df
  // map col => get these columns into an array
  // for the above array, get only the elements matching indexes from ind array
  val postDF1 = csvDF.select(ind map csvDF.columns map csvDF.col: _*)
  // add file name column
  val postDF = postDF1.withColumn("fileName", udfFileName(input_file_name()))

  // create a single column
  //concatenate, using smooch operator. converts a Seq of fieldNames to cols -> vararg
  val monolithDF = postDF.withColumn("monolith",
    concat_ws("|", postDF.schema.fieldNames.map(fName => col(fName)): _*)).
    select(col("monolith"))

  monolithDF.printSchema
  monolithDF.show(20, truncate = false)

  // write to HDFS with a single reducer

}
