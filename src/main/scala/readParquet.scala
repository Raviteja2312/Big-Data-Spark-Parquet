import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j._
import org.apache.spark.sql.functions._
object readParquet {

  def main(args: Array[String]) {

    val conf= new SparkConf().setAppName("parquet example")
  val spark = SparkSession.builder.config(conf).getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val input= spark.read.parquet("file:///Users/ravitejacheruvu/IdeaProjects/ravizparquetexample/userdata3.parquet").cache()

    input.printSchema()

    input.createOrReplaceTempView("parquet_table")

    spark.sql("select count(*) from parquet_table").show()

    input.groupBy("gender").count().show()

    input.where("email=cc").show()

    input.where(input.col("country") ==="Russia").groupBy("country").count().show()

    val input1 = input.withColumn("raviz_column",lit(9).cast("Int"))


    input1.take(10).foreach(println)

    input1.write.parquet("file:///Users/ravitejacheruvu/IdeaProjects/ravizparquetexample/raviz.parquet")

  }

}
