package ass4

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}

object Q2 {
  def main(args: Array[String]) {
    val empPath = "file:///tmp/emps-1.txt" // Should be some file on your system

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val emp = sc.textFile(empPath).cache()
    val sqlRdd = emp.map(_.split(", "))

    val rows = sqlRdd.map(r => Employee(r(0), r(1).toInt, r(2).toFloat))

    rows.toDF().registerTempTable("employee")

    val view = sqlContext.sql("SELECT name FROM employee WHERE salary >= 3500")
    view.write.mode(SaveMode.Overwrite).parquet("/tmp/employee.parquet")

    sc.stop()
  }
}

final case class Employee(name: String, age: Int, salary: Float)