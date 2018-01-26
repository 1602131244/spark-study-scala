package cn.spark.study.core.wordcount
import org.apache.spark.SparkConf
/**
 * Word单词统计
 * GYJ
 * 2018-1-25
 */
import org.apache.spark.SparkContext
object WordCount {
  def main (args:Array[String]) :Unit={
    val conf = new SparkConf()
                   .setMaster("local")
                   .setAppName("WordCount")
    val sc = new SparkContext(conf)
    val lines =sc.textFile("C://Users//104515//Desktop//hadoop.txt", 1)
    val words = lines.flatMap(line => line.split(","))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.foreach(wordCount => println("("+wordCount._1+","+wordCount._2+")"))
    
  }
}