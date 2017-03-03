package ass5

import org.apache.spark.streaming.kafka._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import java.sql.Timestamp
import java.text.SimpleDateFormat
// KafkaConsumer
object Q4Consumer {
  type Q1Result = (String, (Timestamp, Int))
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setAppName("Count Buys").setMaster("local[2]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val kafkaStream = KafkaUtils.createStream(ssc, 
    "127.0.0.1:2181", "Qi", Map("q4" -> 2))
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    val orders: DStream[Array[Order]] = kafkaStream.map{
      line => 
        line._2.split(";").map{
          l =>
            val s = l.split(",")
            Order(new Timestamp(dateFormat.parse(s(0)).getTime()), s(3), s(4).toInt)
        }
    }

    orders.transform{
      rdd =>
        rdd.flatMap(x => x)
    }.map[Q1Result](o => (o.symbol, (o.time, o.amount)))
      .repartition(1)
      .reduceByKey{case (x, y) => (x._1, x._2 + y._2)}
      .reduce{case (x, y) => if(x._2._2 > y._2._2) x else y}.saveAsTextFiles("file:///tmp/qi/ouput", "txt")

    ssc.start()
    ssc.awaitTermination
  }
}

case class Order(time: Timestamp, symbol:String, amount:Int)

