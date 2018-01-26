package cn.spark.study.core.action
import org.apache.spark.{SparkConf,SparkContext}
/**
 * action
 * GYJ
 * 2018-1-26
 */
object ActionOperation {
  def main(args:Array[String]){
    //reduce
    //collect
    //count
    //take
    //saveAsTextFile
    countByKey
  }
  /**
   * reduce对数字进行累加
   */
  def reduce(){
    val conf = new SparkConf()
                  .setAppName("reduce")
                  .setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numbers, 1)
    val sum = numberRDD.reduce(_+_)
    print("List's total is :" + sum)
  }
  /**
   * collect分布式集群拉取数据到本地，然后for循环遍历
   */
  def collect(){
    val conf = new SparkConf()
                  .setAppName("reduce")
                  .setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numbers, 1)
    val doubleNumberRDD = numberRDD.map(number => number*2)
    val doubleNumbers = doubleNumberRDD.collect()
    for(number <- doubleNumbers){
      println("Array :" +number)
    }
  }
  /**
   * count 统计文本有多少个行元素
   */
  def count(){
    val conf = new SparkConf()
                  .setAppName("count")
                  .setMaster("local")
    val sc = new SparkContext(conf)
    val lines =sc.textFile("C://Users//104515//Desktop//spark脚本数据//spark.txt", 1)
    val count = lines.count()
    println("TextFile hava " + count+ " lines .")
  }
  /**
   * take ,从远程集群上获取数据的前N个数据
   */
  def take(){
    val conf = new SparkConf()
                  .setAppName("take")
                  .setMaster("local")
    val sc = new SparkContext(conf)
    val numberList = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numberList, 1)
    val top3Numbers = numberRDD.take(3)
    for(number <- top3Numbers){
      println("Array : " + number)
    }
    
  }
  
  /**
   * saveAsTextFile直接将文件保存到HDFS文件系统或者磁盘上，这里制定的文件夹也就是目录
   */
  def saveAsTextFile(){
    val conf = new SparkConf()
                  .setAppName("saveAsTextFile")
                  .setMaster("local")
    val sc = new SparkContext(conf)
    val numberList = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numberList, 1)
    val doubleNumbers = numberRDD.map(number => number*2)
    doubleNumbers.saveAsTextFile("C://Users//104515//Desktop//double_number.txt")
  }
  
  /**
   * countByKey()，统计每个班学生的人数，也就是对每个Key对应的元素个数
   */
  def countByKey(){
    val conf = new SparkConf()
                  .setAppName("countByKey")
                  .setMaster("local")
    val sc = new SparkContext(conf)
    val scoresList = Array(
        ("112011","leo"),
        ("112012","jack"),
        ("112011","marry"),
        ("112012","tom"),
        ("112012","david"))
    val students = sc.parallelize(scoresList, 1)
    val studentCounts = students.countByKey()
    for(count <- studentCounts){
      println(count._1 + "=================="+count._2)
    }
    
  }
}