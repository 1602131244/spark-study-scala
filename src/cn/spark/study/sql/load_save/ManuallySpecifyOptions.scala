package cn.spark.study.sql.load_save
import org.apache.spark.sql.SparkSession
/**
 * 手动制定数据源类型
 * GYJ 
 * 2018-1-29
 */
object ManuallySpecifyOptions {
  def main(args:Array[String]) : Unit= {
    val spark = SparkSession
                .builder
                .master("local")
                .appName("ManuallySpecifyOptions")
                .config("spark.some.config.option","some-value")
                .getOrCreate()
    val peopleDF = spark.read.format("json").load("C://Users//104515//Desktop//spark脚本数据//people.json")
    peopleDF.printSchema()
    peopleDF.show()
    peopleDF.select("name", "age").write.format("parquet").save("C://Users//104515//Desktop//spark脚本数据//people_name_age.parquet")
  }
}