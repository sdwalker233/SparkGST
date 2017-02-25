package GST

import scala.collection.mutable.ArrayBuffer
import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GST").set("spark.driver.maxResultSize", "3G")
      //.setMaster("local[4]") //local
    val sc = new SparkContext(conf)
    val sumCores = conf.getInt("spark.executor.cores", 8) * conf.getInt("spark.executor.instances", 8)
    val partitioner = new HashPartitioner(sumCores * 2)

    //Get paths of input, output and temp
    var inputPath = if (args.length > 0) args(0) else "/"
    if (inputPath(inputPath.length - 1) != '/') inputPath = inputPath + '/'
    val outputPath = if (args.length > 1) args(1) else inputPath
    val tempPath = if (args.length > 2) args(2) else inputPath

    //Read a directory of text files from HDFS
    var filesRDD = sc.wholeTextFiles(inputPath, sumCores * 2)
    if (filesRDD.partitions.length < sumCores) filesRDD = filesRDD.repartition(sumCores * 2)

    //Remove '\n' and convert text to the format of (character, filename:startPosition)
    val S = filesRDD.flatMap { filePair =>
      val filename = filePair._1.replaceFirst(inputPath, "")
      val content = filePair._2.replaceAll("\n", "")
      content.zipWithIndex.map { charPair =>
        (charPair._1.toString, filename + ":" + charPair._2)
      } ++ Seq(("$" + filename, filename + ":$"))
      //Add a terminal character which is like "$filename" to each file
    }

    //Create the Broadcast variable of text contents
    val broadcastText = sc.broadcast(S.collect())
    val charNum = broadcastText.value.length

    /** Construct the Generalized Suffix Tree
      * First filter the suffixes starting with terminal character
      * Then map the suffixes to a link(root to this suffix, also a tree) with key of the first characters
      * Finally reduce by key, combine the trees
      * */
    val SuffixTree = sc.parallelize(0 until charNum - 1, sumCores * 2)
      .filter(i => broadcastText.value(i)._1.length == 1)
      .map { i =>
        val root = new SuffixNode("", -1, -1, broadcastText)
        val node = new SuffixNode(broadcastText.value(i)._1, i, charNum - 1, broadcastText)
        node.terminal = broadcastText.value(i)._2
        root.children += node
        (broadcastText.value(i)._1, root)
      }.reduceByKey(partitioner, _.combineSuffixTree(_))

    //Get all the terminal characters of suffix trees by key(start character)
    val resRDD = SuffixTree.flatMap { case (_, root) =>
      val resultOfSubTree = new ArrayBuffer[String]()
      root.output(0, resultOfSubTree)
      resultOfSubTree
    }

    //Output the result
    resRDD.saveAsTextFile(outputPath)
  }
}
