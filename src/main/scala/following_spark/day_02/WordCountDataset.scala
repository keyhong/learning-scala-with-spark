package following_spark.day_02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

/** Count up how many of each word appears in a bokk as simply as possible. */
object WordCountDataset {
  
  // case class 정의
  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessarty to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    // Read each line of my book into an Dataset
    // 표준 내장 라이브러리.
    // 1. Spark에서 제공하는 데이터 타입 및 함수에 접근 가능
    // 2. DataFrame, Dataset 등의 구조 활용 가능
    // 3. 데이터 변환 및 조작을 위한 편리한 메서드 사용 가능
    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book] // Book 타입으로 캐스팅
    
    // Split into words separated by a space character
    
    // "$" 기호는 Apache Spark의 Scala API에서 DataFrame의 열을 참조하기 위해 사용되는 기호
    // 오타나 컴파일 시간에 감지되지 않는 열 이름의 오류를 방지

    // "explode" 는 열 또는 컬렉션을 펼쳐서 각 요소를 별도의 행으로 변환하는 작업을 수행

    // "=!=" 는 두 개의 값을 비교하여 동등하지 않음을 나타내는 함수
    val words = input
      .select(explode(split($"value", " ")).alias("word"))
      .filter($"word" =!= "")

    // Count up the occurences of each word
    val wordCounts = words.groupBy("word").count()

    // Show the results.
    wordCounts.show(wordCounts.count.toInt)
  }
}