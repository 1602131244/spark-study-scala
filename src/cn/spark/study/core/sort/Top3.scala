package cn.spark.study.core.sort
import org.apache.spark.{SparkConf,SparkContext}
/**
 * 取最大的前三个数
 * GYJ
 * 2018-1-26
 */
object Top3 {
  def main(args:Array[String]) : Unit ={
    val conf = new SparkConf()
                   .setAppName("Top3")
                   .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C://Users//104515//Desktop//spark脚本数据//top.txt", 1)
    val pairs = lines.map(line => (line.toInt,line))
    val sortPairs = pairs.sortByKey(false)
    val sortNumbers = sortPairs.map(t => t._2)
    val top3 = sortNumbers.take(3)
    for (number <- top3){
      println("List Top 3 : " + number)
    }
  }
}