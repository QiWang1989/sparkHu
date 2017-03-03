package ass5

import org.apache.spark._
import org.apache.spark.sql.SaveMode

object Q2_part2 {
  type Q2Result = (String, Int)

  def main(args: Array[String]): Unit = {
    val path = "hdfs:///tmp/top5InRdd*/part*" // Should be some file on your system

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

//    val stocks = sc.textFile(path).cache()
    val stocks = sc.textFile("hdfs:///tmp/top5InRdd*/part*").cache()
    val sqlRdd = stocks.map{x=> x.substring(1, x.length - 2).split(",")}
    val rows = sqlRdd.map(r => Stock(r(0), r(1).toInt))

    rows.toDF().registerTempTable("Stocks")

    val view = sqlContext.sql("SELECT * FROM Stocks order by amount desc limit 5")
    view.write.mode(SaveMode.Overwrite).parquet("/tmp/stock.parquet")

    sc.stop()
  }
}

final case class Stock(symbol:String, amount:Int)


// ls -l ./*.txt/part-00000 | awk '{if ($5 != 0) print $9}'