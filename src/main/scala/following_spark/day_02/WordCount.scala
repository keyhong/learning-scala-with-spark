package following_spark.scala.day_02

import org.apache.spark._
import org.apache.log4j._

object WordCount {

  /** Out main function where the action happens */

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")

    // Read each line of my book into an RDD
    // main.scala 아래의 폴더가 루트인 줄 알았는데, 어떻게 상위 폴더인 data부터 시작?
    val input = sc.textFile("data/book.txt")

    // Split into word separated by a space character
    val words = input.flatMap(x => x.split(" "))

    // Count up the occurences of each word
    val wordCounts = words.countByValue()

    // Print the results.
    wordCounts.foreach(println)
  }

}