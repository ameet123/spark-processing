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
                          inputDirs: Array[String], schemaFile: String) {

  import java.io.File
  import java.security.MessageDigest

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.{StructType, _}
  import spark.implicits._

  import scala.util._

  val METADATA_END_INDEX = 4
  val METADATA_FILE_NAME = "meta.csv"
  val DATA_FORMAT = "csv"
  val DELIM = "|"
  val IIDR_OUT_DIR = "iidr_post"
  val POST_COL_CNT = 1180
  val PK_COL_CNT = 6
  // private vars
  private var _monolithDF: DataFrame = _
  private var _postDF: DataFrame = _
  private var _finalDF: DataFrame = _
  private var _csvDF: DataFrame = _
  private var indAr: Array[Int] = _

  def indArray: Array[Int] = indAr

  def postdf: DataFrame = _postDF

  def finaldf: DataFrame = _finalDF


  def csvdf: DataFrame = _csvDF

  def monolithDF: DataFrame = _monolithDF

  // *** Functions
  /**
    * based on the command line args, generate a schema structure
    *
    * @param metaFile file containing schema information
    * @return
    */
  def getSchema(metaFile: String): StructType = {

    val metaDF = spark.read.format(DATA_FORMAT).option("header", "true").option("delimiter", "|").load(metaFile)
    StructType(
      metaDF.map(r => r.getString(0)).collect().
        map(c => Try(StructField(c, StringType))).filter(_.isSuccess).map(_.get)
    )
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

  def cleanseStringData: (String => String) = { x => {
    if (x == null) {
      x
    } else {
      x.replace("\"", "").replace("\f", "").replaceAll("^\\s+", "").replaceAll("\\s+$", "")
    }
  }
  }

  def generatePKHash: (String => String) = { s => {
    new String(MessageDigest.getInstance("MD5").digest(s.getBytes))
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
    //    val listOfInputDirs = sc.getConf.get("spark.driver.args").split("\\s+")

    // option("inferSchema", "true") option("header","true").
    _csvDF = spark.read.format(DATA_FORMAT).
      option("delimiter", DELIM).
      load(inputDirs: _*)

    indAr = createColIndexArray(_csvDF)
    /**
      * had this as well
      * //    val postDF3 = postDF2.select(postDF2.columns.map(c => regexp_replace(postDF2.col(c), "\\s+$", "").alias(c)): _*)
      * //    val postDF4 = postDF3.select(postDF3.columns.map(c => regexp_replace(postDF3.col(c), "^\\s+", "").alias(c)): _*)
      * //    val postDF6 = postDF4.select(postDF4.columns.map(c => trim(postDF4.col(c)).alias(c)): _*)
      **/
    val removeDoubleQuotes = udf((x: String) => {
      if (x == null) {
        x
      } else {
        x.replace("\"", "").replace("\f", "").replaceAll("^\\s+", "").replaceAll("\\s+$", "").trim
      }
    })

    // get matching columns from DF
    // csvDF.columns => get all columns from df
    // map col => get these columns into an array
    // for the above array, get only the elements matching indexes from ind array
    //    val postDF1 = csvDF.select(ind map csvDF.columns map csvDF.col: _*)
    _postDF = _csvDF.select(indAr map _csvDF.columns map _csvDF.col: _*)

    // can checkpoint if needed
    //    _postDF.checkpoint(false)
    val postDF2 = _postDF.select(_postDF.columns.map(c => removeDoubleQuotes(_postDF.col(c)).alias(c)): _*)
    val postDF7 = postDF2.withColumn("fileName", udfFileName(input_file_name()))

    val pkInd = (0 to PK_COL_CNT).toArray
    val pkHashUDF = udf(generatePKHash)
    _finalDF = postDF7.withColumn("pk_hash", pkHashUDF(concat_ws("~", pkInd map postDF7.columns map col: _*)))
    _finalDF = postDF2

    // create a single column
    //concatenate, using smooch operator. converts a Seq of fieldNames to cols -> vararg
    val monolithAddedDF = _finalDF.
      withColumn("monolith", concat_ws("|", _finalDF.schema.fieldNames.map(fName => _finalDF.col(fName)): _*))
    _monolithDF = monolithAddedDF.select(monolithAddedDF.col("monolith"))

    // write to HDFS with a single reducer
    _monolithDF.write.text(IIDR_OUT_DIR)
  }
}
