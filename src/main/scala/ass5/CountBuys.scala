package ass5

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import java.sql.Timestamp
import scala.concurrent.duration._

object CountBuys {
  type Q1Result = (String, (Timestamp, Int))

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Count Buys")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val filestream = ssc.textFileStream("hdfs:///tmp/streams")
    case class Order(time: Timestamp, orderId:Long, clientId:Long, symbol:String, amount:Int, price:Double, buy:Boolean)
    import java.text.SimpleDateFormat
    val orders: DStream[Order] = filestream.flatMap{line => 
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val s = line.split(",")
      try {
        assert(s(6) == "B" || s(6) == "S")
        List(Order(new Timestamp(dateFormat.parse(s(0)).getTime()), s(1).toLong, s(2).toLong, s(3), s(4).toInt, s(5).toDouble, s(6) == "B"))
      } catch {
        case e : Throwable => println("Wrong line format ("+e+"): "+line)
          List.empty[Order]
      }
    }
    
    orders.map[Q1Result](o => (o.symbol, (o.time, o.amount)))
      .repartition(1)
      .reduceByKey{case (x, y) => (x._1, x._2 + y._2)}
      .reduce{case (x, y) => if(x._2._2 > y._2._2) x else y}.saveAsTextFiles("file:///tmp/qi/ouput", "txt")

    ssc.start()
    ssc.awaitTermination
  }
}

// ls -l ./*.txt/part-00000 | awk '{if ($5 != 0) print $9}'