package cn.spark.study.core.broadcast
import org.apache.spark.{SparkConf,SparkContext}
/**
 * 广播变量
 * GYJ
 * 2016-1-26
 */
object BroadcastVariable {
  def main(args:Array[String]){
    val conf = new SparkConf()
                   .setAppName("BroadcastVariable")
                   .setMaster("local")
    val sc = new SparkContext(conf)
    val factor =3
    val factorBroadcast = sc.broadcast(factor)
    val numberList = Array(1,2,3,4,5)
    val numberRDD = sc.parallelize(numberList, 1)
    val multipleNumbers = numberRDD.map(num => num * factorBroadcast.value)
    multipleNumbers.foreach(num => println(num))
    
  }
}