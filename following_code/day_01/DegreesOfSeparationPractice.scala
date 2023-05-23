package day_01

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
    
    
  }


}