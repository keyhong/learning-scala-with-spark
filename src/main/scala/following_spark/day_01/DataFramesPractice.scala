package following_spark.day_01

import org.apache.spark.sql.functions._
import org.apache.log4j._

object DataFramesPractice {
  
  // case class 문은 클래스에 대한 속성들의 스키마를 정의할 때 사용할 수 있다!
  case class Person(ID: Int, name: String, age: Int, numFriends: Int)

  def mapper(line: String): Person = {
    val fields = line.split(',')

    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)

    person
  }

  def main(args: Array[String]): Unit = {
        
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.INFO)
    
    // Ues new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appname("SparkSQL")
      .master("local[*")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")  // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    // Convert our csv file to a DataSet, using Person case
    // class to infer the schema
    // Object SparkSession이 아닌 Class SparkSession 을 임포트한다.. Why?
    import spark.implicits._
    val lines = spark.sparkContext.textFile("data/fakefriends.csv")
    /**
     * 1. text 파일을 통으로 읽어서, 라인별 구분자(",")를 mapping 한다.
     * 2. 이후 toDS (to DataSet) 으로 변환 
     * 3. cache()로 로드한 데이터를 메모리에 올린다
     */
    val people = lines.map(mapper).toDS().cache()
        
    // There are lots of other ways to make a DataFrame
    // For example, spark.read.json
    // or sqlContext.table("Hive table name")

    println("Here is our inferred schema:")
    people.printSchema()

    println("Let's select the name column:")
    people.select("name").show()

    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()

    println("Group by age:")
    people.groupBy("age").count().show()

    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show() // select에는 functions.col() 없이 연산 가능?

    spark.stop() // spark stop 명령어로 세션종료가 가능하다
    
    }
}