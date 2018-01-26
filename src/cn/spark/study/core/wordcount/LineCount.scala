package cn.spark.study.core.wordcount
/**
 * 统计每行出现的次数
 * GYJ
 * 2018-1-25
 */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object LineCount {
  
  def main(args:Array[String]) :Unit ={
    val conf =new SparkConf()
                  .setAppName("LineCount")
                  .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C://Users//104515//Desktop//spark脚本数据//hadoop.txt", 1)
    val pairs = lines.map(line => (line,1))
    val lineCounts = pairs.reduceByKey(_+_)
    lineCounts.foreach(lineCount => println(lineCount._1+" is apperas " + lineCount._2 + " times .") )
  }
}