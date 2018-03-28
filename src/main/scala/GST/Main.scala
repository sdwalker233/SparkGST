package GST

import scala.collection.mutable.ArrayBuffer
import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val TASK_MUL = if (args.length > 2) args(2).toInt else 7
    val MAX_PREFIX_LEN = if (args.length > 3) args(3).toInt else 4

    val conf = new SparkConf()
      .setAppName("GST")
      .set("spark.driver.maxResultSize", "4G")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.set("spark.kryoserializer.buffer.max", "2047m")
    //.set("spark.storage.memoryFraction", "0.4") //storage内存百分比(0.6)
    //.set("spark.shuffle.memoryFraction", "0.4") //shuffle内存百分比(0.2)
    //.set("spark.shuffle.file.buffer", "64k") //shuffle溢写磁盘缓冲大小(32k)
    //.set("spark.reducer.maxSizeInFlight", "96m") //shuffle拉取数据缓冲大小(48m)
    //.set("spark.shuffle.manager", "hash")//shuffle类型(sort)
    //.set("spark.shuffle.consolidateFiles", "true")//hash consolidate机制(false)
    //.set("spark.executor.cores", "4")
    //.setMaster("local[4]") //local
    conf.registerKryoClasses(Array(
      classOf[GST.SuffixNode],
      classOf[scala.collection.mutable.ArrayBuffer[SuffixNode]],
      classOf[Array[Long]],
      classOf[Map[Long, Boolean]]
    ))

    val sc = new SparkContext(conf)
    val sumCores = conf.getInt("total.executor.cores", 64)
    val partitionNum = sumCores * TASK_MUL //256

    //Get paths of input, output and temp
    var inputPath = args(0)
    if (!inputPath.endsWith("/")) inputPath = inputPath + '/'
    var outputPath = args(1)
    if (!outputPath.endsWith("/")) outputPath = outputPath + '/'

    //Read a directory of text files from HDFS
    val filesRDD = sc.wholeTextFiles(inputPath, partitionNum).map { filePair =>
      (filePair._1.replaceFirst(inputPath, ""), filePair._2.replaceAll("\n", ""))
    }
    filesRDD.persist()

    //Get filenames and make it a broadcast variable
    val filenamesRDD = filesRDD.map(_._1)
    val bcFilename = sc.broadcast(filenamesRDD.collect())
    //Get Map[filename, fileId(index in filenames)] and make it a broadcast variable
    val bcFilenameMap = sc.broadcast(bcFilename.value.zipWithIndex.toMap)
    val bcFilenameToLengthMap = sc.broadcast(filesRDD.map { filePair =>
      (filePair._1, filePair._2.length)
    }.collect().toMap)

    //Remove '\n' and convert text to the format of RDD[Int]
    val StringRDD = filesRDD.flatMap { filePair =>
      val filename = filePair._1
      val fileId = bcFilenameMap.value(filename)
      val content = filePair._2
      content.zipWithIndex.map { charPair =>
        charPair._1.toInt
      } ++ Seq(fileId + 128)
      //Add a terminal character which is "fileId+128" to each file
    }
    filesRDD.unpersist()

    //Create the Broadcast variable of text contents
    val bcText = sc.broadcast(StringRDD.collect())
    val charNum = StringRDD.count().toInt //bcText.value.length

    //Get Start Position of each file in the whole string
    val bcStartPosition = sc.broadcast(StringRDD.zipWithIndex
      .filter(_._1 >= 128)
      .map { case (fileId, pos) =>
        (fileId - 128, pos.toInt - bcFilenameToLengthMap.value(bcFilename.value(fileId - 128)))
      }.collect().sortBy(-_._1))

    //Pretreatment to determine which prefix can be a key
    val indexRDD = sc.parallelize(0 until charNum - 1, partitionNum)
      .filter(i => bcText.value(i) < 128)
    val prefixRDD = indexRDD.flatMap { i =>
      var prefixKey = bcText.value(i).toLong
      val prefixList = new ArrayBuffer[(Long, Set[Int])]()
      var j = 1
      var breakLoop = false

      while (j < MAX_PREFIX_LEN && !breakLoop) {
        val ch = bcText.value(i + j)
        val prefixPair = (prefixKey, Set(ch))
        prefixList += prefixPair
        prefixKey = prefixKey << 7 | ch
        if (ch > 128) {
          breakLoop = true
        }
        j += 1
      }
      prefixList
    }.reduceByKey(_ ++ _).mapValues(_.size > 1)
    val splitAtFirst = prefixRDD.collectAsMap()

    /** Construct the Generalized Suffix Tree
      * First filter the suffixes starting with terminal character
      * Then map the suffixes to a chain(root to this suffix, also a tree) with key of the first 1~L characters
      * Finally reduce by key, combine the trees
      * */
    val SuffixChainRDD = sc.parallelize(0 until charNum - 1, partitionNum)
      .filter(i => bcText.value(i) < 128)
      .map { i =>
        val root = new SuffixNode(-1, -1, bcText)

        var prefixKey = 0L
        var j = 0
        var breakLoop = false
        var ch = 0

        while (j < MAX_PREFIX_LEN && !breakLoop) {
          ch = bcText.value(i + j)
          prefixKey = prefixKey << 7 | ch
          if (ch >= 128 || !splitAtFirst(prefixKey)) {
            breakLoop = true
          }
          j += 1
        }
        j -= 1
        val node = new SuffixNode(i + j, charNum - 1, bcText)
        node.terminalInfo = i
        root.children += node
        if (ch >= 128) {
          (j.toLong, root)
        }
        else {
          (prefixKey, root)
        }
      }

    //Create partitioner for reduceByKey
    val partitioner = new HashPartitioner(partitionNum)
    //Combine the trees
    val SuffixTreeRDD = SuffixChainRDD.reduceByKey(partitioner, _.combineSuffixTree(_))

    //Get all the terminal characters of suffix trees by key(prefix)
    val resultRDD = SuffixTreeRDD.flatMap { case (key, root) =>
      var startDeep = key
      if (key >= MAX_PREFIX_LEN) {
        startDeep = 0
        var tmp = key
        while (tmp > 128) {
          tmp >>= 7
          startDeep += 1
        }
      }
      val resultOfSubTree = new ArrayBuffer[(Int, Int)]()
      root.output(startDeep.toInt, resultOfSubTree)
      resultOfSubTree
    }.map { case (deep, terminalInfo) =>
      //Get information of the substring as tmp(fileId, deep of prefix)
      val tmp = bcStartPosition.value.find(_._2 <= terminalInfo).getOrElse((1, 1))
      //println(terminalInfo,tmp)
      val fileId = tmp._1
      val terminalPosition = terminalInfo - tmp._2
      deep + " " + bcFilename.value(fileId) + ":" + terminalPosition
    }

    //Output the result
    resultRDD.saveAsTextFile(outputPath)
  }
}
