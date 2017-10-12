/**
  * will take 3 arguments, the 3rd is the array of input hdfs/local dirs to process
  *
  * How to Run:
  * val iidr = new IIDRSparkProcessing(sc,spark, inputArray)
  * iidr.run
  *
  * @param sc        spark context to be passed frmo spark shell
  * @param spark     sparkSession
  * @param inputDirs list of input local/hdfs dirs
  */
class IIDRSparkProcessing(sc: org.apache.spark.SparkContext, spark: org.apache.spark.sql.SparkSession,
                          inputDirs: Array[String]) {

  import java.io.File

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions.{concat_ws, input_file_name, udf}
  import org.apache.spark.sql.types.StructType

  val METADATA_END_INDEX = 1
  val METADATA_FILE_NAME = "meta.dat"
  val DATA_FORMAT = "csv"
  // *** Functions
  /**
    * based on the command line args, generate a schema structure
    *
    * @param metaFile file containing schema information
    * @return
    */
  def getSchema(metaFile: String): StructType = {
    import org.apache.spark.sql.types._
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
    * @param myDF data frame to get index out of
    * @return
    */
  def createColIndexArray(myDF: DataFrame): Array[Int] = {
    val ind1 = (0 to METADATA_END_INDEX).toArray
    val ind2 = ((myDF.columns.length - 2) / 2 until myDF.columns.length).toArray
    ind1 ++ ind2
  }

  /**
    * extract file name from abs path
    * needs to be like this, basic function is not liked by udf...
    *
    * @return
    */
  def getMatchingFileName: (String => String) = { s => {
    val f = new File(s)
    f.getName
  }
  }

  def run(): Unit = {
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
    val monolithAddedDF = postDF.withColumn("monolith",
      concat_ws("|", postDF.schema.fieldNames.map(fName => postDF.col(fName)): _*))
    val monolithDF = monolithAddedDF.select(monolithAddedDF.col("monolith"))

    monolithDF.printSchema
    monolithDF.show(20, truncate = false)

    // write to HDFS with a single reducer
  }
}
