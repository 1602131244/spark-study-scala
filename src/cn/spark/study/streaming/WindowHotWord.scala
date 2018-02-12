package cn.spark.study.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext,Seconds}
/**
 * 热点搜索词滑动统计，每隔10秒
 * GYJ
 * 2018-2-11
 */
object WindowHotWord {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf()
                   .setAppName("WindowHotWord")
                   .setMaster("local[2]")
                   
    val ssc = new StreamingContext(conf,Seconds(5))
    val searchLogDstream = ssc.socketTextStream("spark1", 9999)
    
    //将搜索日志转换成，只有一个搜索词即可
    
    val searchWordDstream = searchLogDstream.map(_.split(" ")(0))
    
    
    //将搜索词映射为（searchWord，1） 的tuple2模式
    
    val searchWordPairDstream = searchWordDstream.map(searchWord => (searchWord,1))
    
    //执行reduceByKeyAndWindow，滑动窗口操作
    
    val searchWordCountDstream = searchWordPairDstream
                  .reduceByKeyAndWindow((v1 :Int, v2 :Int) => v1+v2, 
                                        Seconds(60) , 
                                        Seconds(10) )
                  
    
                  
    //执行transform操作，因为，一个窗口，就是一个60秒的数据，会变成一个RDD，然后，对这一个RDD
		//根据每隔搜索词的词频进行排序，然后获取排名前三的热点搜索词
                  
    val finalDstream = searchWordCountDstream.transform(searchWordCountsRDD =>  {
      
      //执行搜索词和出现频率的反转
      val countSearchWordRDD = searchWordCountsRDD.map(tuple => (tuple._2,tuple._1))
       
      //执行降序排序
      
      val sortedCountSearchWordRDD = countSearchWordRDD.sortByKey(false)
      
      //再次反转，<String，Integer>格式
      
      val sortedSearchWordCountRDD = sortedCountSearchWordRDD.map(tuple => (tuple._2,tuple._1))
      
      
      //然后用take()获取排名前三的数据
      
      val top3SearchWordCounts = sortedSearchWordCountRDD.take(3)
      for(tuple <- top3SearchWordCounts){
        println(tuple)
      }
      
      searchWordCountsRDD
    })

    finalDstream.print()
    
    
    
    ssc.start()
    ssc.awaitTermination()
  }
}