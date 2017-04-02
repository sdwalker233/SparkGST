package GST
/*
Check whether the result is right
Separately run
 */
import scala.io._

object check {
  def readFile(filename:String): Map[String, Long] ={
    val lines = Source.fromFile(filename).getLines()
    lines.map { line =>
      val a = line.split(" ")
      a(1) -> a(0).toLong
    }.toMap
  }

  def main(args: Array[String]) {
    val filename1 = if(args.length > 0) {args(0)} else
      "/Users/shad0w_walker/Desktop/cloud/exset/res3"
    val filename2 = if(args.length > 1) {args(1)} else
      "/Users/shad0w_walker/Desktop/cloud/res/part-00000"

    val arr1 = readFile(filename1)
    val arr2 = readFile(filename2)
    var count = 0
    val d = arr1.keys.toSet -- arr2.keys.toSet
    d.foreach(println(_))
    arr1.foreach { pair =>
      if (arr2(pair._1) != pair._2) count += 1
    }
    println(count)
  }
}