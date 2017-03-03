package ass5

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import java.sql.Timestamp
import scala.concurrent.duration._

object Q2 {
  type Q2Result = (String, Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Count Buys").setMaster("local[2]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val filestream = ssc.textFileStream("hdfs:///tmp/streams")
    case class Order(time: Timestamp, orderId:Long, clientId:Long, symbol:String, amount:Int, price:Double, buy:Boolean)
    import java.text.SimpleDateFormat
    val orders: DStream[Order] = filestream.flatMap(line => {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val s = line.split(",")
      try {
        assert(s(6) == "B" || s(6) == "S")
        List(Order(new Timestamp(dateFormat.parse(s(0)).getTime()), s(1).toLong, s(2).toLong, s(3), s(4).toInt, s(5).toDouble, s(6) == "B"))
      } catch {
        case e : Throwable => println("Wrong line format ("+e+"): "+line)
          List.empty[Order]
      }
    })

    val top5InRDD = orders.map[Q2Result](o => (o.symbol, o.amount))
      .repartition(1)
      .reduceByKey(_ + _).transform{
      rdd =>
        ssc.sparkContext.parallelize(rdd.sortBy(_._2, false).take(5))
    }

    top5InRDD.foreachRDD{
      (rdd, time) =>
        if(!rdd.isEmpty())
          rdd.saveAsTextFile(s"file:///tmp/top5InRdd$time")
          rdd.saveAsTextFile(s"hdfs:///tmp/top5InRdd$time")
    }

//    top5InRDD.reduce{case (x, y) => if(x._2 > y._2) x else y}.
//    ssc.sparkContext.parallelize(top5InRDD.reduce{case (x, y) => if(x._2 > y._2) x else y}
//      .slice(Time((2 seconds).toMillis), Time((199 seconds).toMillis))
//      .flatten(rdd => rdd.collect()).sortBy(_._2).take(5)).saveAsTextFile("file:///tmp/top5InAll")

    ssc.start()
    ssc.awaitTermination//OrTimeout((300 seconds).toMillis)
  }
}

// ls -l ./*.txt/part-00000 | awk '{if ($5 != 0) print $9}'