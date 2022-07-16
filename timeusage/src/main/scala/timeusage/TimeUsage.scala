package timeusage

import org.apache.spark.sql.*
import org.apache.spark.sql.types.*
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Logger, Level}
import scala.util.Properties.isWin

/** Main class */
object TimeUsage extends TimeUsageInterface:
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions.*

  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Time Usage")
  lazy val sc: SparkContext = new SparkContext(conf)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .master("local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits.{StringToColumn, localSeqToDatasetHolder}
  import scala3encoders.given

  /** Main function */
  def main(args: Array[String]): Unit =
    timeUsageByLifePeriod()
    spark.close()

  def timeUsageByLifePeriod(): Unit =
    val (columns, initDf) = read("src/main/resources/timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    val finalDf = timeUsageGrouped(summaryDf)
    finalDf.show()

  /** @return The read DataFrame along with its column names. */
  def read(path: String): (List[String], DataFrame) =
    val df = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(path)
    (df.schema.fields.map(_.name).toList, df)

  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row =
    Row((line.head.toString :: line.tail.map(_.toDouble)): _*)

  /** @return The initial data frame columns partitioned in three groups: primary needs (sleeping, eating, etc.),
    *         work and other (leisure activities)
    *
    * @see https://www.kaggle.com/bls/american-time-use-survey
    *
    * The dataset contains the daily time (in minutes) people spent in various activities. For instance, the column
    * “t010101” contains the time spent sleeping, the column “t110101” contains the time spent eating and drinking, etc.
    *
    * This method groups related columns together:
    * 1. “primary needs” activities (sleeping, eating, etc.). These are the columns starting with “t01”, “t03”, “t11”,
    *    “t1801” and “t1803”.
    * 2. working activities. These are the columns starting with “t05” and “t1805”.
    * 3. other activities (leisure). These are the columns starting with “t02”, “t04”, “t06”, “t07”, “t08”, “t09”,
    *    “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
    */
  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) =
    val primaryNeedsColumnsPrefixes = scala.collection.Set("t01", "t03", "t11", "t1801", "t1803")
    val workColumnsPrefixes = scala.collection.Set("t05", "t1805")
    val otherColumnsPrefixes = scala.collection.Set("t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18")

    var primaryNeedsColumns = List[Column]()
    var workColumns = List[Column]()
    var otherColumns = List[Column]()

    columnNames.foreach(name =>
      val prefix = name.substring(0,3)
      val longerPrefix = if name.length() <=5 then name.substring(0,name.length()) else name.substring(0,5) 
      val column: Column = col(name)

      if primaryNeedsColumnsPrefixes.contains(prefix) || primaryNeedsColumnsPrefixes.contains(longerPrefix) then primaryNeedsColumns = column :: primaryNeedsColumns
      else if workColumnsPrefixes.contains(prefix) || workColumnsPrefixes.contains(longerPrefix) then workColumns = column :: workColumns
      else if otherColumnsPrefixes.contains(prefix) || prefix.equals("t18") then otherColumns = column :: otherColumns
      )

    (primaryNeedsColumns, workColumns, otherColumns)

  /** @return a projection of the initial DataFrame such that all columns containing hours spent on primary needs
    *         are summed together in a single column (and same for work and leisure). The “teage” column is also
    *         projected to three values: "young", "active", "elder".
    *
    * @param primaryNeedsColumns List of columns containing time spent on “primary needs”
    * @param workColumns List of columns containing time spent working
    * @param otherColumns List of columns containing time spent doing other activities
    * @param df DataFrame whose schema matches the given column lists
    *
    * This methods builds an intermediate DataFrame that sums up all the columns of each group of activity into
    * a single column.
    *
    * The resulting DataFrame would have the following columns:
    * - working: value computed from the “telfs” column of the given DataFrame:
    *   - "working" if 1 <= telfs < 3
    *   - "not working" otherwise
    * - sex: value computed from the “tesex” column of the given DataFrame:
    *   - "male" if tesex = 1, "female" otherwise
    * - age: value computed from the “teage” column of the given DataFrame:
    *   - "young" if 15 <= teage <= 22,
    *   - "active" if 23 <= teage <= 55,
    *   - "elder" otherwise
    * - primaryNeeds: sum of all the `primaryNeedsColumns`, in hours
    * - work: sum of all the `workColumns`, in hours
    * - other: sum of all the `otherColumns`, in hours
    *
    * Finally, the resulting DataFrame would exclude people that are not employable (ie telfs = 5).
    */
  def timeUsageSummary(
    primaryNeedsColumns: List[Column],
    workColumns: List[Column],
    otherColumns: List[Column],
    df: DataFrame
  ): DataFrame =
    // Transform the data from the initial dataset into data that make
    // more sense for our use case
    val workingStatusProjection: Column = when(df("telfs") >= 1 && df("telfs") < 3, "working").otherwise("not working").as("working")
    val sexProjection: Column = when(df("tesex") === 1, "male").otherwise("female").as("sex")
    val ageProjection: Column = when(df("teage") >= 15 && df("teage") <= 22, "young")
                                .when(df("teage") >= 23 && df("teage") <= 55, "active")
                                .otherwise("elder").as("age")

    // Create columns that sum columns of the initial dataset
    val primaryNeedsProjection: Column = (primaryNeedsColumns.foldLeft(lit(0))((col1, col2) => col1 + df(col2.toString()))/60).as("primaryNeeds")
    val workProjection: Column = (workColumns.foldLeft(lit(0))((col1, col2) => col1 + df(col2.toString()))/60).as("work")
    val otherProjection: Column = (otherColumns.foldLeft(lit(0))((col1, col2) => col1 + df(col2.toString()))/60).as("other")
    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryNeedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force

  /** @return the average daily time (in hours) spent in primary needs, working or leisure, grouped by the different
    *         ages of life (young, active or elder), sex and working status.
    * @param summed DataFrame returned by `timeUsageSumByClass`
    *
    * The resulting DataFrame would have the following columns:
    * - working: the “working” column of the `summed` DataFrame,
    * - sex: the “sex” column of the `summed` DataFrame,
    * - age: the “age” column of the `summed` DataFrame,
    * - primaryNeeds: the average value of the “primaryNeeds” columns of all the people that have the same working
    *   status, sex and age, rounded with a scale of 1 (using the `round` function),
    * - work: the average value of the “work” columns of all the people that have the same working status, sex
    *   and age, rounded with a scale of 1 (using the `round` function),
    * - other: the average value of the “other” columns all the people that have the same working status, sex and
    *   age, rounded with a scale of 1 (using the `round` function).
    *
    * Finally, the resulting DataFrame would be sorted by working status, sex and age.
    */
  def timeUsageGrouped(summed: DataFrame): DataFrame =
    summed
      .groupBy("working", "sex", "age")
      .agg(round(avg($"primaryNeeds"), 1).as("primaryNeeds"),
          round(avg($"work"), 1).as("work"),
          round(avg($"other"), 1).as("other"))
      .orderBy("working", "sex", "age")

  /**
    * @return Same as `timeUsageGrouped`, but using a plain SQL query instead
    * @param summed DataFrame returned by `timeUsageSumByClass`
    */
  def timeUsageGroupedSql(summed: DataFrame): DataFrame =
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(timeUsageGroupedSqlQuery(viewName))

  /** @return SQL query equivalent to the transformation implemented in `timeUsageGrouped`
    * @param viewName Name of the SQL view to use
    */
  def timeUsageGroupedSqlQuery(viewName: String): String =
    s"SELECT working, sex, age, round(AVG(primaryNeeds), 1) AS primaryNeeds, round(AVG(work), 1) AS work, round(AVG(other), 1) AS other " + 
    s"FROM ${viewName} " +
    s"GROUP BY working, sex, age " +
    s"ORDER BY working, sex, age"

  /**
    * @return A `Dataset[TimeUsageRow]` from the “untyped” `DataFrame`
    * @param timeUsageSummaryDf `DataFrame` returned by the `timeUsageSummary` method
    */
  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
    timeUsageSummaryDf.map{rowElem => 
                            TimeUsageRow(rowElem.getAs[String]("working"),
                                        rowElem.getAs[String]("sex"),
                                        rowElem.getAs[String]("age"),
                                        rowElem.getAs[Double]("primaryNeeds"),
                                        rowElem.getAs[Double]("work"),
                                        rowElem.getAs[Double]("other"))}

  /**
    * @return Same as `timeUsageGrouped`, but using the typed API when possible
    * @param summed Dataset returned by the `timeUsageSummaryTyped` method
    *
    * Note that, though they have the same type (`Dataset[TimeUsageRow]`), the input
    * dataset contains one element per respondent, whereas the resulting dataset
    * contains one element per group (whose time spent on each activity kind has
    * been aggregated).
    */
  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = 
    val groupedDS = summed.groupByKey(timeUsageRow => (timeUsageRow.working, timeUsageRow.sex, timeUsageRow.age))
    val aggDS = groupedDS.agg(
                              round(avg($"primaryNeeds"), 1).as[Double],
                              round(avg($"work"), 1).as[Double],
                              round(avg($"other"), 1).as[Double])
    aggDS
          .map{case (key: (String, String, String), col1: Double, col2: Double, col3: Double) => 
                TimeUsageRow(key._1, key._2, key._3, col1, col2, col3)
          }.orderBy("working", "sex", "age")

/**
  * Models a row of the summarized data set
  * @param working Working status (either "working" or "not working")
  * @param sex Sex (either "male" or "female")
  * @param age Age (either "young", "active" or "elder")
  * @param primaryNeeds Number of daily hours spent on primary needs
  * @param work Number of daily hours spent on work
  * @param other Number of daily hours spent on other activities
  */
case class TimeUsageRow(
  working: String,
  sex: String,
  age: String,
  primaryNeeds: Double,
  work: Double,
  other: Double
)
