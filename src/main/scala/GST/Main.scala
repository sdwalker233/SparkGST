package GST

import scala.collection.mutable.ArrayBuffer
import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val taskMul = 2
    val conf = new SparkConf()
      .setAppName("GST")
      .set("spark.driver.maxResultSize", "4G")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer","256m")
      //.setMaster("local[4]") //local
    conf.registerKryoClasses(Array(
      classOf[GST.SuffixNode],
      classOf[scala.collection.mutable.ArrayBuffer[SuffixNode]],
      classOf[scala.Array[(Int, Int, Int)]]
    ))

    val sc = new SparkContext(conf)
    val sumCores = conf.getInt("spark.executor.cores", 8) * conf.getInt("spark.executor.instances", 8)
    val partitioner = new HashPartitioner(sumCores * taskMul)

    //Get paths of input, output and temp
    var inputPath = if (args.length > 0) args(0) else "/"
    if (!inputPath.endsWith("/")) inputPath = inputPath + '/'
    val outputPath = if (args.length > 1) args(1) else inputPath
    val tempPath = if (args.length > 2) args(2) else inputPath

    //Read a directory of text files from HDFS
    var filesRDD = sc.wholeTextFiles(inputPath, sumCores * taskMul)
    if (filesRDD.partitions.length < sumCores) filesRDD = filesRDD.repartition(sumCores * taskMul)
    filesRDD.persist()

    //Get filenames and make it a broadcast variable
    val filenamesRDD = filesRDD.map(_._1.replaceFirst(inputPath, ""))
    val bcFilename = sc.broadcast(filenamesRDD.collect())
    //Get Map[filename, fileId(index in filenames)] and make it a broadcast variable
    val bcFilenameMap = sc.broadcast(bcFilename.value.zipWithIndex.toMap)

    //Remove '\n' and convert text to the format of (character, fileId, startPosition)(Int, Int, Int)
    val S = filesRDD.flatMap { filePair =>
      val filename = filePair._1.replaceFirst(inputPath, "")
      val fileId = bcFilenameMap.value(filename)
      val content = filePair._2.replaceAll("\n", "")
      content.zipWithIndex.map { charPair =>
        (charPair._1.toInt, fileId, charPair._2)
      } ++ Array((-fileId, fileId, -1))
      //Add a terminal character which is "-fileId" to each file
    }
    filesRDD.unpersist()

    //Create the Broadcast variable of text contents
    val bcText = sc.broadcast(S.collect())
    val charNum = bcText.value.length
    
    /** Construct the Generalized Suffix Tree
      * First filter the suffixes starting with terminal character
      * Then map the suffixes to a link(root to this suffix, also a tree) with key of the first characters
      * Finally reduce by key, combine the trees
      * */
    val SuffixTree = sc.parallelize(0 until charNum - 1, sumCores * taskMul)
      .filter(i => bcText.value(i)._1 > 0)
      .map { i =>
        val root = new SuffixNode(-charNum, -1, -1, bcText)
        val node = new SuffixNode(bcText.value(i)._1, i, charNum - 1, bcText)
        node.terminal = (bcText.value(i)._2, bcText.value(i)._3)
        root.children += node
        (bcText.value(i)._1, root)
      }.reduceByKey(partitioner, _.combineSuffixTree(_))

    //Get all the terminal characters of suffix trees by key(start character)
    val resRDD = SuffixTree.flatMap { case (_, root) =>
      val resultOfSubTree = new ArrayBuffer[(Int, Int, Int)]()
      root.output(0, resultOfSubTree)
      resultOfSubTree
    }.map { case (deep, fileId, terminalPosition) =>
      deep + " " + bcFilename.value(fileId) + "$" + terminalPosition
    }

    //Output the result
    resRDD.saveAsTextFile(outputPath)
  }
}
