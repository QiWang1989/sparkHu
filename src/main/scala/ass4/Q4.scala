package ass4

import argonaut.Argonaut._
import argonaut._
import ass4.Address._
import ass4.CustomerAddress._
import org.apache.spark.{SparkConf, SparkContext}

object Q4 {
  def main(args: Array[String]) {
    val customersPath = "file:///tmp/customers.csv" // Should be some file on your system

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("drop table customers")
    sqlContext.sql(
      """
        |CREATE TABLE customers (id INT, name STRING, addresses map<string,struct<street_1:string,street_2:string,city:string,state:string,zip_code:string>>)
        |row FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        |WITH SERDEPROPERTIES (
        |   "separatorChar" = ",",
        |   "quoteChar"     = "\""
        |)
      """.stripMargin)

    sqlContext.sql(s"LOAD DATA LOCAL INPATH '$customersPath' INTO TABLE customers")

    val shipping = sqlContext.sql(
      """
        |SELECT addresses from customers
      """.stripMargin).map{
      r =>
        r.getString(0).decodeOption[CustomerAddress].get.shipping
    }.collect()

    shipping.foreach(println)
    println(s"Number of unique shipping addresses: ${shipping.distinct.length}")

    sc.stop()
  }
}

final case class CustomerAddress(shipping: Address, billing: Address)
object CustomerAddress{
  implicit def CustomerAddressCodecJson: CodecJson[CustomerAddress] =
    casecodec2(CustomerAddress.apply, CustomerAddress.unapply)("shipping", "billing")
}
final case class Address(street_1: String, street_2: String, city: String, state: String, zip_code: String){
  override def toString: JsonField = s"$street_1 $street_2, $city, $state $zip_code"
}
object Address {
  implicit def AddressCodecJson: CodecJson[Address] =
    casecodec5(Address.apply, Address.unapply)("street_1", "street_2", "city", "state", "zip_code")
}