package following_spark.day_01

import org.apache.spark.sql._
import org.apache.log4j

object DataFramesDatasetPractice {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    // inferSchema 옵션은 스키마를 자동으로 추론한다. 근데 as 로 타입을 정의는 why?
    import spark.implicits.day_01
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
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
    people.select(people("name"), people("age") + 10).show()
    
    spark.stop()
  }

}