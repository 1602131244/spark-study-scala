package cn.spark.study.sql
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.Dataset
/**
 * 使用JSon文件创建DataFrame
 * GYJ
 * 2018-1-29
 */
object DataFrameCreate {
  
  def main (args:Array[String]):Unit = {
    val spark = SparkSession
                .builder()
                .appName("DataFrameCreate")
                .master("local")
                .config("spark.some.config.option","some-value")
                .getOrCreate()
                
    val df =spark.read.json("C://Users//104515//Desktop//spark脚本数据//students.json");
    df.show();
  }
}