package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.*
import org.apache.log4j.{Logger, Level}

import org.apache.spark.rdd.RDD
import scala.util.Properties.isWin

case class WikipediaArticle(title: String, text: String):
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)

object WikipediaRanking extends WikipediaRankingInterface:
  // Reduce Spark logging verbosity
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setAppName("wikipediarankingalgo").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.lines.map(WikipediaData.parse(_))).cache()

  /** Returns the number of articles on which the language `lang` occurs.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int =
    rdd.aggregate(0)((a,b) => if b.mentionsLanguage(lang) then a+1 else a, (a1,a2) => a1+a2) 

  /* Computes the ranking of the languages (`val langs`) by determining the number 
   * of Wikipedia articles that mention each language at least once. 
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = 
    langs.map(a => (a, occurrencesOfLang(a, rdd))).sortWith((a,b) => a._2 > b._2 )

  /* Computes an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = 
    rdd
      .flatMap(a => langs.map(b => if a.mentionsLanguage(b) then (b, Iterable(a)) else (b, Iterable[WikipediaArticle]())))
      .reduceByKey((v1,v2) => v1 ++ v2)

  /* Computes the language ranking again, but now using the inverted index.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = 
    index
        .mapValues(v => v.size)
        .collectAsMap()
        .toList
        .sortWith((a,b) => a._2 > b._2 )

  /* Computes the language ranking again using aggregation by key.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = 
    rdd
      .flatMap(a => langs.map(b => if a.mentionsLanguage(b) then (b, Iterable(a)) else (b, Iterable[WikipediaArticle]())))
      .aggregateByKey(0)((a,b) => a + b.size, (a1, a2) => a1 + a2)
      .collectAsMap()
      .toList
      .sortWith((a,b) => a._2 > b._2)

  def main(args: Array[String]): Unit =

    /* Languages ranked naively */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked using reduction */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T =
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
