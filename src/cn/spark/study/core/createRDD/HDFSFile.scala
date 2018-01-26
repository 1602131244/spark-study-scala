package cn.spark.study.core.createRDD
import org.apache.spark.{SparkConf,SparkContext}
/**
 * 使用HDFS文件创建RDD
 * 案例：统计文本文件字数
 * GYJ
 * 2018-1-26
 */
object HDFSFile {
  
  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
                   .setAppName("HDFSFile")
                   
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://spark1:9000/spark.txt", 1)
    val lineLeng = lines.map(num => num.length)
    val count = lineLeng.reduce(_+_)
    println("HDFS 文件文本总字数： " + count )
  }
}