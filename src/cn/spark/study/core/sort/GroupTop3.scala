package cn.spark.study.core.sort
import org.apache.spark.{SparkConf,SparkContext}
/**
 * 分组统计：对每个班级内的学生成绩，去前三名（分组去Top N ）
 * GYJ 
 * 2018-1-26
 */
object GroupTop3 {
  def main(args:Array[String]) : Unit = {
    val conf = new SparkConf()
                   .setAppName("GroupTop3")
                   .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("C://Users//104515//Desktop//spark脚本数据//score.txt", 1)
    val pairs = lines.map{line => 
      val lineSplited = line.split(" ")                                       
      (lineSplited(0),lineSplited(1).toInt)
    }.groupByKey()
    
    val top3Scores = pairs.map(classScore => 
      {
        val top3 = Array(-1,-1,-1)
        val className = classScore._1
        val scores = classScore._2
        for (score <- scores){
          import scala.util.control._
          val loop = new Breaks;
          loop.breakable{
            for (i <- 0 until 3) {  
              if (top3(i) == -1) {  
                top3(i) = score
                loop.break
              } else if (score > top3(i)) {  
                var j = 2  
                while (j > i) {  
                  top3(j) = top3(j - 1)
                  j = j - 1 
                }  
                top3(i) = score
                loop.break
              }  
            }  
          }
        }
        (className,top3)     
      })
      top3Scores.foreach(classScore => {
        println("className : " + classScore._1)
        val scores = classScore._2
        for(score <- scores){
          println("score : " +score)
          
        }
        println("================================")
      })
    
    
  }
}