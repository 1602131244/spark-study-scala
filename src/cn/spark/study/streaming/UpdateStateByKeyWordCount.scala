package cn.spark.study.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Durations}

/**
 * 基于UpdateStateByKey算子实现缓存机制的实时WordCount程序
 * GYJ
 * 2018-1-30
 */
object UpdateStateByKeyWordCount {
  
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf()
                   .setMaster("local[2]")
                   .setAppName("UpdateStateByKeyWordCount")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    ssc.checkpoint("hdfs://spark1:9000/wordcount_checkpoint_scala")
    val lines =ssc.socketTextStream("spark1", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts=pairs.updateStateByKey((values:Seq[Int],state:Option[Int])=>{
      //创建一个变量，用于记录单词出现次数
      var newValue=state.getOrElse(0) //getOrElse相当于if....else.....
      for(value <- values){
        newValue +=value //将单词出现次数累计相加
      }
      Option(newValue)
    })
    
    wordCounts.print
    
    
    ssc.start
    ssc.awaitTermination
    
  }
}