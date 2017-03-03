package ass5

import java.time.Instant
import java.util.Properties
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

// KafkaProducer
object Q4 {
  def main(args: Array[String]): Unit = {
    val p = new Properties()
    p.put("bootstrap.servers", "localhost:9092")
    p.put("acks", "all")
    p.put("retries", 0: java.lang.Integer)
    p.put("batch.size", 16384: java.lang.Integer)
    p.put("linger.ms", 1: java.lang.Integer)
    p.put("buffer.memory", 33554432: java.lang.Integer)
    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaJavaProducer = new KafkaProducer[String, String](p)


    val filename = "/Users/wangqi8/Documents/harvard/assignments/orders.txt"
    val lines = Source.fromFile(filename).getLines().grouped(1000)

    lines.foreach{ln =>
      kafkaJavaProducer.send(
        new ProducerRecord[String, String](
          "q4",
          Instant.now().getEpochSecond.toString,
          ln.mkString(";")),
        new org.apache.kafka.clients.producer.Callback() {
          override def onCompletion(
                                     metadata: RecordMetadata,
                                     exception: Exception): Unit = {
            println(s"Sent to offset ${metadata.offset()}")
          }
        }
      )

      Thread.sleep(1000)
    }

  }
}

