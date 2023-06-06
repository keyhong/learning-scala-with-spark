package following_spark.day_01

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apach.log4j._
import scala.collection.mutable.ArrayBuffer

/** Finds the degrees of separation between two Marvel comic book charactors, based
  * on co-appearances in a comic.
  */

object DegressOfSeparation {

  // The characters we want to find the separation between.
  val startCharacterID: Int = 5306 // SpiderMan
  val targetCharacterID: Int = 14 // ADAM 3,031 (who?)

  // We make our accumulator a "global" Option so we can reference it in a mappler later
  /** "Option" 은 값이 있거나 또는 없거나(None) 상태. 파이썬은 Optional과 같다.
    * 값이 하위 타입은 Some[T], 없는 타입은 None
    * 연속 계산에서 안정성을 갖추기 위해 실행 
    */ 

  /** 태스크별 집계 누적 연산을 위한 공유 변수 Accumulator
    * 누적될 수 있고, 클러스터의 작업자(worker)는 +- 를 통해 값을 추가
    * 드라이버(driver) 프로그램만 값에 액세스 가능
    */
  var hitCounter: Option[LongAccumulator] = None

  // Some custom data types
  // BFSData contains an array of hero ID connections, the distance, and color.
  // 파이썬에서 쓰는 typevar(from typing impor TypeVar)와 유사하게 타입명을 정의 가능
  type BFSData = (Array[Int], Int, String)

  //A BFSNode has a hereID and the BFSData associated with it.
  type BFSNode = (Int, BFSData)

  /** Convert a line of of raw input into a BFSNode */
  def converToBFS(line: String): BFSNode = {

    // Split up the line into fields
    // "\\s+" : 하나 이상의 공백 문자 시퀀스
    // String.split => return : Array[String] 
    val fields = line.split("\\s+")

    // Extract this hero ID from the first field
    // scala에서 인덱스 접근은 객체명(i)로 접근
    // toInt와 같은 타입 변환 메서드는 끝에 ()를 안붙이는 것 같다. Why? 인스턴스 내부 속성값인가?
    val heroID = fields(0).toInt

    /**
      * ArrayBuffer : Java 에서의 ArrayList
      * Array는 고정길이지만, ArrayBuffer는 가변길이이다.
      * 스칼라 표준 라이브러리. mutable하다 (초기할당, elem이 늘면 더 큰 메모리 객체로 복사)
      */
    val connections: ArrayBuffer[Int] = ArrayBuffer()

    // Extract subsequent here ID's into the connections array
    /**
      * until은 마지막 숫자를 포함하지 않는 range (파이썬의 range와 비슷)
      * 1부터 fields 까지의 index를 connections(ArrayBuffer)에 넣는다
      */
    for ( connection <- 1 until (fields.length - 1)) {
        connections += fields(connection).toInt
    }

    // Default distance and color is 9999 and white
    var color: String = "WHITE"
    var distance: Int = 999

    // Unless this is the character we're starting from
    // 파이썬과 달리 조건절에서 () 로 조건문을 묶어준다.
    if (heroID == startCharacterID) {
        color = "GRAY"
        distance = 0
    }

    (heroID, (connections.toArray, distance, color))
  }

  /** Create "iteration 0" of our RDD of BFSNodes */
  def createStartingRdd(sc: Sparkcontext): RDD[BFSNode] = {
    val inputFile = sc.textFile("data/marvel-graph.txt")
    inputFile.map(converToBFS)
  }

  /** Expands as BFSNode into this node and its children */
  def bfsMap(node: BFSNode): Array[BFSNode] = {
    
    // Extract data from the BFSNode
    val characterID: int = node._1
    val data: BFSData = node._2

    val connections: Array[Int] = data._1
    val distance: Int = data._2
    val color: String = data._3
    
    // This is called from flatMap, so we return an array
    // of potentially many BFSNodes to add to our ne RDD
    var resutls: ArrayBuffer[BFSNode] = ArrayBuffer()

    // Gray nodes are flagged for expansion, and create new
    // gray nodes for each connection
    if (color == "GRAY") {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor =  "GRAY"

        // Have we stumbled across the character we're looking for?
        // If so increment out accumulator so the driver script knows.

        if (targetCharacterID == connection) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }

        // Create our new Gray node for this connection and add it to the results
        val newEntry: BFSNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }

      // Color this node as black, indicating it has been processed already.      
      color = "BLACK"
    }

    // Add the original node back in, so its connections can get merged with
    // the gray nodes in the reducer.
    val thisEntry: BFSNode = (characterID, (connections, distance, color))
    results += thisEntry
  }

  /** Combine nodes for the same heroID, preserving the shortest length and darkest color. */
  def bfsReduce(data1: BFSData, data2: BFSData2): BFSData {

    // Extract data that we are combining
    val edges1: Array[Int] = data1._1
    val edges2: Array[Int] = data2._1
    val distance1: Int = data1._2
    val distance2: Int = data2._2
    val color1: String = data1._3
    val color2: String = data2._3

    // Default node values
    var distance: Int = 9999
    var color: String = "WHITE"
    var edges: ArrayBuffer[Int] = ArrayBuffer()

    // See if one is the original node with its connections.
    // If so preserve them.
    if (edges1.length > 0) {
      // ++= 는 가변(mutable) 컬렉션 요소에 사용 : ListBuffer, ArrayBuffer, StringBuilder 등
      // ++ 은 불변(immutable) 컬렉션 요소에 사용 : List, Vector
      edges ++= edges1
    }
    if (edges.lnegth > 0) {
      edges ++= edges2
    }

    // Preserve minimum distance
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }

    // Preserve darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }
    if (color1 == "GRAY" && color2 == "GRAY") {
      color = color1
    }
    if (color1 == "BLACK" && color2 == "BLACK") {
      color = color1
    }

    // toArray : Scala 컬렉션을 배열(Array)로 변환하는 메서드
    (edges.toArray, distance, color)
  }


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "DegreesOfSeparation")

    // Our accumulator, used to signal when we find the target
    // character in our BFS traversal.
    // Some은 scala의 옵션 타입 (값이 존재 할 수도 있고 없을 수도 있음)
    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    val iterationRdd = createStartingRdd(sc)

    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)

      // Create new vertices as needed to darken or reduce distances in th reduce stage.
      // If we encounter the node we're looking for as a GRAY node,
      // increment our accumulator to signal that we're done.
      
      // flatMap은 함수형 프로그래밍에서 주로 사용되는 고차함수(Higher-Order Function)
      // 각 요소에 대해 주어진 함수를 적용하고, 그 결과를 단일 컬렉션으로 평면화(flatten)
      val mapped = iterationRdd.flatMap(bfsMap)

      // Note that mappred.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.
      println("Processing " + mapped.count() + " values.")

      if (hitCount.isDefined) {
        val hitCount = hitCount.get.values
        if (hitCount > 0) {
          // 한 줄 띄어서 출력 가능
          println("Hit the target character! From " + hitCount +
              " difference directions(s).")

          return 
        }
      }

      // Reducer combines data for each character ID, preserving the darkest
      // color and shortest path.
      iterationRdd = mapped.reduceByKey(bfsReduce)
    }
  }
}