package cn.spark.study.sql
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.Dataset
/**
 * DataFrame 的常用操作
 * GYJ
 * 2018-1-29
 */
object DataFrameOpreation {
  def main(args:Array[String]):Unit ={
    val spark = SparkSession
                .builder()
                .appName("DataFrameOpreation")
                .master("local")
                .config("spark.some.config.option","some-value")
                .getOrCreate()
    val df = spark.read.json("C://Users//104515//Desktop//spark脚本数据//students.json")
    
    //打印DataFrame的所有数据
    df.show
    
    //打印DataFrame的元数据（schema）
    df.printSchema
    
    //查询某列所有的数据
    
    df.select("name").show
    
    //查询某几列所有的数据，并对列进行计算
    
    df.select(df.col("name"), df.col("age").plus(1)).show()
    
    //根据某一列的值进行锅炉
    
    df.filter(df.col("age").gt(18)).show()
    
    //根据某一列进行分组，然后进行聚合
    
    df.groupBy(df.col("age")).count.show()
  }
}