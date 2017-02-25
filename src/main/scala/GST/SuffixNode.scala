package GST

import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.broadcast.Broadcast

class SuffixNode (var startChar:String,
                  var start:Int, var end:Int,
                  val broadcastText:Broadcast[Array[(String, String)]]
                 ) extends Serializable{
  var children = new ArrayBuffer[SuffixNode]()
  var terminal:String = ""

  def len(): Int ={
    end - start + 1
  }

  //Get the first character of the node
  def calStartChar(): String ={
    startChar = broadcastText.value(start)._1
    startChar
  }

  //Calculate the same part of the string of two nodes
  def compareSuffix(link:SuffixNode): Int = {
    val Len = if(len < link.len) len() else link.len()
    for(i <- 1 to Len) {
      if(broadcastText.value(start+i-1)._1 != broadcastText.value(link.start+i-1)._1){
        return i-1
      }
    }
    Len
  }

  //Combine a Suffixtree 'tree' to this
  def combineSuffixTree(tree:SuffixNode): SuffixNode = {
    for (child2 <- tree.children) {
      breakable {
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
              val node = new SuffixNode(child1.startChar, child1.start, child1.end, broadcastText)
              child2.start += sharedLen
              child2.calStartChar()
              node.children += child2
              child1.combineSuffixTree(node)
            }
            //child2 is on the link between this and child1
            else if (sharedLen == child2.len) {
              val node = new SuffixNode(child2.startChar, child2.start, child2.end, broadcastText)
              child1.start += sharedLen
              child1.calStartChar()
              node.children += child1
              child2.combineSuffixTree(node)
              children -= child1
              children += child2
            }
            //this forks to child1 and child2
            else {
              val node = new SuffixNode(child1.startChar, child1.start, child1.start + sharedLen - 1, broadcastText)
              node.children = ArrayBuffer(child1, child2)
              child1.start += sharedLen
              child2.start += sharedLen
              child1.calStartChar()
              child2.calStartChar()
              children -= child1
              children += node
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
  def output(deep:Int, res:ArrayBuffer[String]): Unit ={
    if(terminal.nonEmpty){
      res += deep+" "+terminal
    }
    children.foreach(_.output(deep+1, res))
  }
}

