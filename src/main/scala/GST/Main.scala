package GST

import scala.collection.mutable.ArrayBuffer
import org.apache.spark._

object Main {
  def main(args: Array[String]) {
    val taskMul = 4
    val conf = new SparkConf()
      .setAppName("GST")
      .set("spark.driver.maxResultSize", "4G")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "2047m")
      .set("spark.storage.memoryFraction", "0.4") //storage内存百分比(0.6)
      .set("spark.shuffle.memoryFraction", "0.4") //shuffle内存百分比(0.2)
      .set("spark.shuffle.file.buffer", "64k") //shuffle溢写磁盘缓冲大小(32k)
      .set("spark.reducer.maxSizeInFlight", "96m") //shuffle拉取数据缓冲大小(48m)
      //.set("spark.shuffle.manager", "hash")//shuffle类型(sort)
      //.set("spark.shuffle.consolidateFiles", "true")//hash consolidate机制(false)
      //.set("spark.executor.cores", "3")
      //.setMaster("local[4]") //local
    conf.registerKryoClasses(Array(
      classOf[GST.SuffixNode],
      classOf[scala.collection.mutable.ArrayBuffer[SuffixNode]],
      classOf[Array[Long]],
      classOf[Map[Int, Boolean]]
    ))

    val sc = new SparkContext(conf)
    val sumCores = conf.getInt("total.executor.cores", 48)
    val partitionNum = sumCores * taskMul //256

    //Get paths of input, output and temp
    var inputPath = if (args.length > 0) args(0) else "/"
    if (!inputPath.endsWith("/")) inputPath = inputPath + '/'
    var outputPath = if (args.length > 1) args(1) else inputPath
    if (!outputPath.endsWith("/")) outputPath = outputPath + '/'
    var tempPath = if (args.length > 2) args(2) else inputPath
    if (!tempPath.endsWith("/")) tempPath = tempPath + '/'

    //Read a directory of text files from HDFS
    val filesRDD = sc.wholeTextFiles(inputPath, partitionNum)
    filesRDD.persist()

    //Get max length of all the textfiles
    val max_len = filesRDD.map(_._2.length).reduce(math.max(_, _))
    val bigfile = max_len > 1000000
    //Get filenames and make it a broadcast variable
    val filenamesRDD = filesRDD.map(_._1.replaceFirst(inputPath, ""))
    val bcFilename = sc.broadcast(filenamesRDD.collect())
    //Get Map[filename, fileId(index in filenames)] and make it a broadcast variable
    val bcFilenameMap = sc.broadcast(bcFilename.value.zipWithIndex.toMap)
    val genomeMap = Map(('A', 1), ('T', 2), ('C', 3), ('G', 4))

    //Remove '\n' and convert text to the format of (character, fileId, startPosition)(Int, Int, Int)
    //Then compress (Int, Int, Int) to Long
    val S = filesRDD.flatMap { filePair =>
      val filename = filePair._1.replaceFirst(inputPath, "")
      val fileId = bcFilenameMap.value(filename)
      val content = filePair._2.replaceAll("\n", "")
      content.zipWithIndex.map { charPair =>
        //Compress (fileId, terminalPosition)(Int, Int) to terminalInfo(Int)
        //Compress (character, terminalInfo)(Int, Int) to a Long variable
        link(charPair._1.toInt, fileId * max_len + charPair._2)
      } ++ Seq(link(fileId + 128, 0))
      //Add a terminal character which is "fileId+128" to each file
    }
    filesRDD.unpersist()

    //Create the Broadcast variable of text contents
    val bcText = sc.broadcast(S.collect())
    val charNum = bcText.value.length

    //Pretreatment to determine which prefix can be a key
    val indexRDD = sc.parallelize(0 until charNum - 1, partitionNum)
      .filter(i => get1(bcText.value(i)) < 128)
    val prefixRDD = indexRDD.flatMap { i =>
      val fi = get1(bcText.value(i))
      val se = get1(bcText.value(i + 1))
      //1-char prefix
      val one = (fi, Set(se))
      if (se < 128) {
        val th = get1(bcText.value(i + 2))
        //2-char prefix
        val two = ((fi << 7) + se, Set(th))
        if (bigfile && th < 128) {
          val fo = get1(bcText.value(i + 3))
          //3-char prefix
          val three = ((fi << 14) + (se << 7) + th, Set(fo))
          if (fo < 128) {
            val fif = get1(bcText.value(i + 4))
            //4-char prefix
            val four = ((fi << 21) + (se << 14) + (th << 7) + fo, Set(fif))
            Seq(one, two, three, four)
          }
          else {
            Seq(one, two, three)
          }
        }
        else {
          Seq(one, two)
        }
      }
      else {
        Seq(one)
      }
    }.reduceByKey(_ ++ _).mapValues(_.size > 1)
    val splitAtFirst = prefixRDD.collectAsMap()

    /** Construct the Generalized Suffix Tree
      * First filter the suffixes starting with terminal character
      * Then map the suffixes to a chain(root to this suffix, also a tree) with key of the first 1~5 characters
      * Finally reduce by key, combine the trees
      * */
    val SuffixChain = sc.parallelize(0 until charNum - 1, partitionNum)
      .filter(i => get1(bcText.value(i)) < 128)
      .map { i =>
        val root = new SuffixNode(0, -1, -1, bcText)
        val fi = get1(bcText.value(i))
        val se = get1(bcText.value(i + 1))
        val th = if (se < 128) get1(bcText.value(i + 2)) else se
        val fo = if (th < 128) get1(bcText.value(i + 3)) else th
        val fif = if (fo < 128) get1(bcText.value(i + 4)) else fo
        if (bigfile && fo < 128 &&
          splitAtFirst((fi << 21) + (se << 14) + (th << 7) + fo) &&
          splitAtFirst((fi << 14) + (se << 7) + th) &&
          splitAtFirst((fi << 7) + se) &&
          splitAtFirst(fi)
        ) {
          val node = new SuffixNode(fif, i + 4, charNum - 1, bcText)
          node.terminalInfo = get2(bcText.value(i))
          root.children += node
          if (fif >= 128) (4, root)
          else ((genomeMap(fi.toChar) << 28) + (se << 21) + (th << 14) + (fo << 7) + fif, root)
        }
        else if (bigfile && th < 128 &&
          splitAtFirst((fi << 14) + (se << 7) + th) &&
          splitAtFirst((fi << 7) + se) &&
          splitAtFirst(fi)
        ) {
          val node = new SuffixNode(fo, i + 3, charNum - 1, bcText)
          node.terminalInfo = get2(bcText.value(i))
          root.children += node
          if (fo >= 128) (3, root)
          else ((fi << 21) + (se << 14) + (th << 7) + fo, root)
        }
        else if (se < 128 &&
          splitAtFirst((fi << 7) + se) &&
          splitAtFirst(fi)
        ) {
          val node = new SuffixNode(th, i + 2, charNum - 1, bcText)
          node.terminalInfo = get2(bcText.value(i))
          root.children += node
          if (th >= 128) (2, root)
          else ((fi << 14) + (se << 7) + th, root)
        }
        else if (splitAtFirst(fi)) {
          val node = new SuffixNode(se, i + 1, charNum - 1, bcText)
          node.terminalInfo = get2(bcText.value(i))
          root.children += node
          if (se >= 128) (1, root)
          else ((fi << 7) + se, root)
        }
        else {
          val node = new SuffixNode(fi, i, charNum - 1, bcText)
          node.terminalInfo = bcText.value(i).toInt
          root.children += node
          (fi, root)
        }
      }

    //Create partitioner for reduceByKey
    val partitioner =
      if (bigfile) new RangePartitioner[Int, SuffixNode](partitionNum, SuffixChain)
      else new HashPartitioner(partitionNum)
    val SuffixTree = SuffixChain.reduceByKey(partitioner, _.combineSuffixTree(_))

    //Get all the terminal characters of suffix trees by key(prefix)
    val resRDD = SuffixTree.flatMap { case (key, root) =>
      var startDeep = key
      if (key > 4) {
        startDeep = 0
        var tmp = key
        while (tmp > 128) {
          tmp >>= 7
          startDeep += 1
        }
      }
      val resultOfSubTree = new ArrayBuffer[(Int, Int)]()
      root.output(startDeep, resultOfSubTree)
      resultOfSubTree
    }.map { case (deep, terminalInfo) =>
      //Extract terminalInfo:Int to (fileId, terminalPosition)
      val fileId = terminalInfo / max_len
      val terminalPosition = terminalInfo % max_len
      deep + " " + bcFilename.value(fileId) + ":" + terminalPosition
    }

    //Output the result
    resRDD.saveAsTextFile(outputPath)
  }

  //Combine two Int into a Long
  def link(a: Int, b: Int): Long = {
    (a.toLong << 32) + b
  }

  //Get the Character(the high 32 digit of the Long variable)
  def get1(x: Long): Int = {
    (x >> 32).toInt
  }

  //Get the terminalInfo(the low 32 digit of the Long variable)
  def get2(x: Long): Int = {
    x.toInt
  }
}