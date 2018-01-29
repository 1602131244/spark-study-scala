package cn.spark.study.sql.parquet
import org.apache.spark.sql.SparkSession
/**
 * Parquet数据源之使用变成方式加载数据
 * GYJ
 * 2018-1-29
 */
object ParquetLoadData {
  def main(args:Array[String]) : Unit= {
    val spark = SparkSession
                .builder
                .master("local")
                .appName("genericLoadSave")
                .config("spark.some.config.option","some-value")
                .getOrCreate()
    val usersDF = spark.read.load("C://Users//104515//Desktop//spark脚本数据//users.parquet")
    usersDF.createOrReplaceTempView("users")
    val userNameDF = spark.sql("select name from users")
    val userNames= userNameDF.rdd.map(row => "Name : "+row.getString(0)).collect()
    
    for(name <- userNames){
      println(name)
    }
    
  }
}