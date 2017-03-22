package GST

import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.broadcast.Broadcast

class SuffixNode (var startChar: Int,
                  var start: Int, var end: Int,
                  val bcText: Broadcast[Array[(Int, Int, Int)]]
                 ) extends java.io.Serializable{
  var children = new ArrayBuffer[SuffixNode]()
  var terminal:(Int, Int) = (-1,-1)

  def len(): Int ={
    end - start + 1
  }

  //Get the first character of the node
  def calStartChar(): Int ={
    startChar = bcText.value(start)._1
    startChar
  }

  //Calculate the same part of the string of two nodes
  def compareSuffix(link:SuffixNode): Int = {
    val Len = if(len < link.len) len() else link.len()
    for(i <- 1 to Len) {
      if(bcText.value(start+i-1)._1 != bcText.value(link.start+i-1)._1){
        return i-1
      }
    }
    Len
  }

  //Combine a Suffixtree 'tree' to this
  def combineSuffixTree(tree:SuffixNode): SuffixNode = {
    breakable {
      for (child2 <- tree.children) {
        var hasChar = false
        for (child1 <- children) {
          if (child1.startChar == child2.startChar) {
            hasChar = true
            //Calculate the same part from beginning of the string of child1 and child2
            val sharedLen = child1.compareSuffix(child2)
            //child1 and child2 is the same node
            if (sharedLen == child1.len && sharedLen == child2.len) {
              child1.combineSuffixTree(child2)
            }
            //child1 is on the link between this and child2
            else if (sharedLen == child1.len) {
              tree.startChar = child1.startChar
              tree.start = child1.start
              tree.end = child1.end
              tree.children.clear()

              child2.start += sharedLen
              child2.calStartChar()
              tree.children += child2
              child1.combineSuffixTree(tree)
            }
            //child2 is on the link between this and child1
            else if (sharedLen == child2.len) {
              tree.startChar = child2.startChar
              tree.start = child2.start
              tree.end = child2.end
              tree.children.clear()

              child1.start += sharedLen
              child1.calStartChar()
              tree.children += child1
              child2.combineSuffixTree(tree)
              children -= child1
              children += child2
            }
            //this forks to child1 and child2
            else {
              tree.startChar = child1.startChar
              tree.start = child1.start
              tree.end = child1.start + sharedLen - 1
              tree.children.clear()

              tree.children = ArrayBuffer(child1, child2)
              child1.start += sharedLen
              child2.start += sharedLen
              child1.calStartChar()
              child2.calStartChar()
              children -= child1
              children += tree
            }
            break
          }
        }
        if (!hasChar) {
          children += child2
        }
      }
    }
    this
  }

  //For debug
  def print(deep:Int): Unit ={
    for(i <- 1 to deep) printf(" ")
    println(startChar,(start,end))
    children.foreach(_.print(deep+1))
  }

  //Get terminal characters of the whole tree
  def output(deep:Int, res:ArrayBuffer[(Int, Int, Int)]): Unit ={
    if(terminal._1 != -1){
      res += ((deep, terminal._1, terminal._2))
    }
    children.foreach(_.output(deep+1, res))
  }
}

