package cn.spark.study.core.sort
import org.apache.spark.{SparkConf,SparkContext}
/**
 * 排序WordCount程序
 * GYJ
 * 2018-1-26
 */
object SortWordCount {
  def main(args:Array[String]) : Unit ={
    val conf = new SparkConf()
                   .setAppName("SortWordCount")
                   .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C://Users//104515//Desktop//spark脚本数据//spark.txt", 1)
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts =pairs.reduceByKey(_+_)
    val countWords = wordCounts.map(t => (t._2,t._1))
    val sortCountWords = countWords.sortByKey(false, 1)
    val sortWordCounts =sortCountWords.map(t =>(t._2,t._1))
    sortWordCounts.foreach(t => println(t._1 + " apperas :" + t._2 + " times . "))
  }
}