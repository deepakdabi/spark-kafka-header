package app

import org.apache.spark.sql.SparkSession

object App {

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger( "org").setLevel(Level.OFF)
  Logger.getLogger( "akka").setLevel(Level.OFF)

  System.setProperty("hadoop.home.dir", "C:\\Users\\Swastik Dabi\\Desktop\\hadoop_winutils\\")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._
//    val someDF = Seq(
//      (8, "bat"),
//      (64, "mouse"),
//      (-27, "horse")
//    ).toDF("number", "word")
    //    someDF.show()

    val kafkaDf = spark.read.format("kafka")
      .option("subscribe", "test")
      .option("includeHeaders", "true")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .load()

    kafkaDf.show()


  }
}
