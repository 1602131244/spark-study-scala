package cn.spark.study.core.sort
import org.apache.spark.{SparkConf,SparkContext}
import cn.spark.study.core.sort.impl.SecondarySortKey
/**
 * 二次排序
 * 1.自定义实现的key,要实现Ordered接口和Serializable接口，在Key中实现自己对多个列的排序算法
 * 2.将包含文本的RDD，映射成可key为自定义key,value为文本的JavaPairRDD
 * 3.使用sortByKey算子，按照自定义的Key进行排序
 * 4.再次映射，提出自定义的key，只保留文本行
 * GYJ
 * 2017-11-03
 */
object SecondarySort {
  def main(args:Array[String]):Unit ={
    val conf = new SparkConf()
                   .setAppName("SecondarySort")
                   .setMaster("local")
    val sc = new SparkContext(conf)
    val lines =sc.textFile("C://Users//104515//Desktop//spark脚本数据//sort.txt", 1)
    val pairs = lines.map(line => {
      val lineSplited = line.split(" ")
      val key = new SecondarySortKey(lineSplited(0).toInt,lineSplited(1).toInt)
      (key,line)
    })
    
    //进行排序
    val sortPairs = pairs.sortByKey(false, 1)
    
    //映射
    
    val sortedLines = sortPairs.map(sortedLine => sortedLine._2)
    
    sortedLines.foreach(line => println("sorted result : " + line))
    //sortedLines.collect().foreach(println)
    
    
    
  }
  
}