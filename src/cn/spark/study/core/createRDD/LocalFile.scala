package cn.spark.study.core.createRDD
import org.apache.spark.{SparkConf,SparkContext}
/**
 * 使用本地文件创建RDD
 * 案例：统计文本文件字数
 * GYJ
 * 2018-1-26
 */
object LocalFile {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
                   .setAppName("LocalFile")
                   .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C://Users//104515//Desktop//hadoop 测试数据//access_2013_05_30.log", 1)
    val lineLeng = lines.map(num => num.length)
    val count = lineLeng.reduce(_+_)
    println("本地文件文件文本总字数： " + count )
  }
}