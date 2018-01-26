package cn.spark.study.core.accumulator
import org.apache.spark.{SparkConf,SparkContext}
/**
 * accumulator累加变量
 * GYJ
 * 2018-1-26
 */
object TestAccumulator {
  
  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
                   .setAppName("TestAccumulator")
                   .setMaster("local")
    val sc = new SparkContext(conf)
    
    
    //创建Accumulator变量
    //调用SparkContext中的accumulator（）方法
    val sumAccumulator = sc.accumulator(0)
    val numberList = Array(1,2,3,4,5);
    val numbers = sc.parallelize(numberList, 1)
    numbers.foreach(number => sumAccumulator +=number)
    print(sumAccumulator.value)
    
  }
}