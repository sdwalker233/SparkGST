package GST
/*
Count by characters to solve the problem of data skew
Separately run
 */
import org.apache.spark._

object charCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("charCount")
      .setMaster("local[4]") //local

    val sc = new SparkContext(conf)

    //Get paths of input, output and temp
    var inputPath = if (args.length > 0) args(0) else //'/'
      "/Users/shad0w_walker/Desktop/cloud/input/genome/2M_100"
    if (!inputPath.endsWith("/")) inputPath = inputPath + '/'
    val outputPath = if (args.length > 1) args(1) else inputPath
    val tempPath = if (args.length > 2) args(2) else inputPath

    //Read a directory of text files from HDFS
    val filesRDD = sc.wholeTextFiles(inputPath, 20)

    val S = filesRDD.flatMap { filePair =>
      val content = filePair._2.replaceAll("\n", "")
      content.map((_, 1))
    }.reduceByKey(_ + _)

    val res = S.collect()
    res.sortBy(-_._2).foreach(println(_))
    //println(res.map(_._2).reduce(_ + _))
  }
}
/*
(T,61064584)
(A,60950279)
(C,43855515)
(G,43844822)
 */