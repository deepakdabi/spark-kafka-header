package app

import org.apache.spark.sql.SparkSession

object App {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .getOrCreate()

    import spark.implicits._
    val someDF = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    ).toDF("number", "word")

    someDF.show()
  }
}
