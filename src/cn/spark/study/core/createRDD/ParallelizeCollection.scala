package cn.spark.study.core.createRDD
import org.apache.spark.{SparkConf,SparkContext}
/**
 * 并行化集合创建RDD
 * 案例：累加1到10
 * GYJ
 * 2018-1-28
 */
object ParallelizeCollection {
  def main(args:Array[String]){
    val conf = new SparkConf()
                   .setAppName("ParallelizeCollection")
                   .setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numbers, 1)
    val sum = numberRDD.reduce(_+_)
    println("1到10累加和： " +sum)
  }
}