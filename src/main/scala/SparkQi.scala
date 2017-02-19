import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkQi {
  def main(args: Array[String]) {
    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

//  Q1
    val logan = sc.textFile("file:///tmp/logan.txt").cache()
    val loganCounts = logan.flatMap(_.split(" ")).map(w => (w, 1))

    val wall = sc.textFile("file:///tmp/theGreatWall.txt").cache()
    val wallCounts = wall.flatMap(_.split(" ")).map(w => (w, 1))

    val stopWords = sc.textFile("file:///tmp/stopWords.txt")
    val stopWordCounts = stopWords.filter(_.nonEmpty).map(w => (w, 1))

    val loganWC = loganCounts.subtract(stopWordCounts).reduceByKey(_ + _)//.sortBy(_._2, false)  .collect().take(10)
    val wallWC = wallCounts.subtract(stopWordCounts).reduceByKey(_ + _)//.sortBy(_._2, false)   .collect().take(10)

    val intersect = loganWC.join(wallWC).collect()
    val n = intersect.length * 0.05
    intersect.take(n.toInt)

    sc.stop()
  }
}
