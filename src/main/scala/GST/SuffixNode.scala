package GST

import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.broadcast.Broadcast

class SuffixNode (var start: Int, var end: Int,
                  val bcText: Broadcast[Array[Int]]
                 ) extends java.io.Serializable {
  var children = new ArrayBuffer[SuffixNode]()
  var terminalInfo: Int = -1

  def len(): Int = {
    end - start + 1
  }

  //Get the first character of the node
  def startChar(): Int = {
    if (start < 0) -1
    else bcText.value(start)
  }

  //Calculate the same part of the string of two nodes
  def compareSuffix(link: SuffixNode): Int = {
    val Len = math.min(len(), link.len())
    if (start == link.start) return Len
    for (i <- 1 to Len) {
      if (bcText.value(start + i - 1) != bcText.value(link.start + i - 1)) {
        return i - 1
      }
    }
    Len
  }

  //Combine a Suffixtree 'tree' to this
  def combineSuffixTree(tree: SuffixNode): SuffixNode = {
    for (child2 <- tree.children) {
      breakable {
        var hasChar = false
        for (child1 <- children) {
          if (child1.startChar == child2.startChar) {
            hasChar = true
            //Calculate the same part from beginning of the string of child1 and child2
            val sharedLen = child1.compareSuffix(child2)
            val new_start = math.min(child1.start, child2.start)
            val new_end = new_start + sharedLen - 1

            //case 1: child1 and child2 is the same node
            if (sharedLen == child1.len && sharedLen == child2.len) {
              child1.combineSuffixTree(child2)
            }
            //case 2: child1 is on the link between this and child2
            else if (sharedLen == child1.len) {
              val node = new SuffixNode(new_start, new_end, bcText)
              child2.start += sharedLen
              node.children += child2
              child1.combineSuffixTree(node)
            }
            //case 3: child2 is on the link between this and child1
            else if (sharedLen == child2.len) {
              val node = new SuffixNode(new_start, new_end, bcText)
              child1.start += sharedLen
              node.children += child1
              child2.combineSuffixTree(node)
              children -= child1
              children += child2
            }
            //case 4: this forks to child1 and child2
            else {
              val node = new SuffixNode(new_start, new_end, bcText)
              node.children = ArrayBuffer(child1, child2)
              child1.start += sharedLen
              child2.start += sharedLen
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
  def print(deep: Int): Unit = {
    for (i <- 1 to deep) printf(" ")
    println(startChar, (start, end))
    children.foreach(_.print(deep + 1))
  }

  //Get terminal characters of the whole tree
  def output(deep: Int, res: ArrayBuffer[(Int, Int)]): Unit = {
    if (terminalInfo != -1) {
      res += ((deep, terminalInfo))
    }
    children.foreach(_.output(deep + 1, res))
  }
}
