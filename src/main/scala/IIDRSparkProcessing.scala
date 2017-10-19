/**
  * will take 3 arguments, the 3rd is the array of input hdfs/local dirs to process
  *
  * How to Run:
  * val iidr = new IIDRSparkProcessing(sc,spark, inputArray)
  * iidr.run
  * tarnsient since running it inside spark-shell causes serialization error on sc
  *
  * @param sc        spark context to be passed frmo spark shell
  * @param spark     sparkSession
  * @param inputDirs list of input local/hdfs dirs
  */
class IIDRSparkProcessing(
                           @transient val sc: org.apache.spark.SparkContext, spark: org.apache.spark.sql.SparkSession,
                           inputDirs: Array[String], schemaFile: String, outDir: String,
                           PK_COL_COUNT: Int) extends java.io.Serializable {

  import java.io.File
  import java.security.MessageDigest

  import org.apache.spark.SparkContext
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.{StructType, _}
  import org.apache.spark.sql.{DataFrame, Row}
  import spark.implicits._

  import scala.util._

  // CRUD operation identifier is in this column
  val OPERATION_COL_INDEX = 2
  val METADATA_END_INDEX = 3
  val METADATA_FILE_NAME = "meta.csv"
  val DATA_FORMAT = "csv"
  val DELIM = "|"
  val IIDR_OUT_DIR = "iidr_post"
  val POST_COL_CNT = 1180
  val PK_HASH_SEP = "~"

  private var postPkStartIndex: Int = 0


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
    postPkStartIndex = (myDF.columns.length - 2) / 2

    println(s"Insert start: $postPkStartIndex :")

    val ind2 = ((myDF.columns.length - 2) / 2 until myDF.columns.length).toArray
    ind1 ++ ind2
  }

  def createPkHash(r: Row): String = {
    var operationPkStartIndex: Int = 0
    var operationPkEndIndex: Int = 0
    val op = r.getString(OPERATION_COL_INDEX)
    //    println(s"OP=$op PK_COL:$PK_COL_COUNT")
    if (op == "I") {
      operationPkStartIndex = postPkStartIndex
      operationPkEndIndex = operationPkStartIndex + PK_COL_COUNT - 1
    } else {
      operationPkStartIndex = METADATA_END_INDEX + 1
      operationPkEndIndex = operationPkStartIndex + PK_COL_COUNT - 1
    }
    //    println(s"Start:$operationPkStartIndex -> End:$operationPkEndIndex")
    val pkArray = (operationPkStartIndex to operationPkEndIndex).toArray
    var colString = new StringBuilder
    var prefix = ""
    for (elem <- pkArray) {
      colString.append(prefix)
      prefix = PK_HASH_SEP
      colString.append(r.getString(elem))
    }
    val pkHash = hashDigest(colString.toString())
    println(s"pk col string:${colString.toString()} pk Hash:$pkHash")
    return pkHash
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

  def hashDigest: (String => String) = { s => {
    MessageDigest.getInstance("MD5").digest(s.getBytes).map("%02x".format(_)).mkString
  }
  }

  def hadoopConf(sc: SparkContext): Unit = {
    sc.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.nameservice1",
      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.nameservice1", "namenode96,namenode157")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.namenode96", "dwbdtest1r1m.wellpoint.com:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.nameservice1.namenode157", "dwbdtest1r2m.wellpoint.com:8020")
  }

  def joinPkCols(r: Row, pkArray: Array[Int]): String = {
    var colString = new StringBuilder
    var prefix = ""
    for (elem <- pkArray) {
      colString.append(prefix)
      prefix = PK_HASH_SEP
      colString.append(r.getString(elem))
    }
    colString.toString()
  }

  val f_hash: (Row) => String = (r: Row) => {
    var operationPkStartIndex: Int = 0
    var operationPkEndIndex: Int = 0
    val op = r.getString(OPERATION_COL_INDEX)
    //      println(s"OP=$op PK_COL:$PK_COL_COUNT")
    if (op == "I") {
      operationPkStartIndex = postPkStartIndex
      operationPkEndIndex = operationPkStartIndex + PK_COL_COUNT - 1
    } else {
      operationPkStartIndex = METADATA_END_INDEX + 1
      operationPkEndIndex = operationPkStartIndex + PK_COL_COUNT - 1
    }
    //      println(s"Start:$operationPkStartIndex -> End:$operationPkEndIndex")
    val pkArray = (operationPkStartIndex to operationPkEndIndex).toArray
    val colString = joinPkCols(r, pkArray)
    val pkHash = hashDigest(colString)
    //      println(s"pk col string:${colString} pk Hash:$pkHash")
    pkHash
  }

  def run(): Unit = {
    hadoopConf(sc)
    // UDFs
    val udfFileName = udf(getMatchingFileName)
//    val f_hash = (r: Row) => {
//      var operationPkStartIndex: Int = 0
//      var operationPkEndIndex: Int = 0
//      val op = r.getString(OPERATION_COL_INDEX)
//      //      println(s"OP=$op PK_COL:$PK_COL_COUNT")
//      if (op == "I") {
//        operationPkStartIndex = postPkStartIndex
//        operationPkEndIndex = operationPkStartIndex + PK_COL_COUNT - 1
//      } else {
//        operationPkStartIndex = METADATA_END_INDEX + 1
//        operationPkEndIndex = operationPkStartIndex + PK_COL_COUNT - 1
//      }
//      //      println(s"Start:$operationPkStartIndex -> End:$operationPkEndIndex")
//      val pkArray = (operationPkStartIndex to operationPkEndIndex).toArray
//      val colString = joinPkCols(r, pkArray)
//      val pkHash = hashDigest(colString)
////      println(s"pk col string:${colString} pk Hash:$pkHash")
//      pkHash
//    }
    val pkHashUDF = udf(f_hash)
    val removeDoubleQuotes = udf((x: String) => {
      if (x == null) {
        x
      } else {
        x.replace("\"", "").replace("\f", "").replaceAll("^\\s+", "").replaceAll("\\s+$", "").trim
      }
    })
    // ** Execution
    _csvDF = spark.read.format(DATA_FORMAT).
      option("delimiter", DELIM).
      option("quote", """""").
      load(inputDirs: _*)

    indAr = createColIndexArray(_csvDF)

    // get matching columns from DF
    // csvDF.columns => get all columns from df
    // map col => get these columns into an array
    // for the above array, get only the elements matching indexes from ind array
    //    val postDF1 = csvDF.select(ind map csvDF.columns map csvDF.col: _*)
    _postDF = _csvDF.select(indAr map _csvDF.columns map _csvDF.col: _*)

    val postDF2 = _postDF.select(_postDF.columns.map(c => removeDoubleQuotes(_postDF.col(c)).alias(c)): _*)
    val postDF7 = postDF2.withColumn("fileName", udfFileName(input_file_name()))

    _finalDF = postDF7.withColumn("pk_hash", pkHashUDF(struct(postDF7.columns.map(postDF7(_)): _*)))
    //     _finalDF = postDF7

    _finalDF.write.format(DATA_FORMAT).save(outDir)
    _finalDF.printSchema
  }
}
