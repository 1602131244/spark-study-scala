package cn.spark.study.sql
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
import org.apache.spark.sql.Row
/**
 * Json数据源
 * GYJ
 * 2018-1-29
 */
object JSONDataSource {
  
  def main(args:Array[String]) : Unit ={
    val conf = new SparkConf()
                   .setMaster("local")
                   .setAppName("JSONDataSource")
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    
    val studentScoresDF = sqlContext.read.json("C://Users//104515//Desktop//spark脚本数据//students1.json")
    
    //注册临时表,查询分数大于80分的学生姓名
    studentScoresDF.createOrReplaceTempView("student_score")
    
    val goodStudentScoresDF = sqlContext.sql("select name,score from student_score where score >= 80")
    
    //将DataFrame 转换为RDD，执行transformation
    //针对包含JSon串的JavaRDD，创建DataFrame
    
    val goodStudentNames = goodStudentScoresDF.rdd.map(row => row(0)).collect()
    
    //然后针对JavaRDD<String> 创建DataFrame
    
    val studentInfoJSONs = Array(
        "{\"name\":\"Leo\",\"age\":18}",
        "{\"name\":\"Marry\",\"age\":17}",
        "{\"name\":\"Jack\",\"age\":19}")
   
    //转换为RDD
    
    val studentINfoRDD = sc.parallelize(studentInfoJSONs, 3)
    
    val studentInfoDF = sqlContext.read.json(studentINfoRDD)
    
    studentInfoDF.createOrReplaceTempView("student_infos")
    
    //拼接SQl
    var sqlString ="select name,age from student_infos where name in ("
    
    for(i <- 0 until goodStudentNames.length){
      sqlString += "'" +goodStudentNames(i) + "'"
      if(i < goodStudentNames.length-1){
        sqlString += ","
      }
    }
    sqlString += ")"
    
    //获取大于80分学生的基本信息
    val goodStudentInfoDF = sqlContext.sql(sqlString)
    
    //然后将两份数据的DataFrame转换为JavaPairRDD，执行transformation
    //将DataFrame转化为RDD，再map为JavaPairRDD，然后进行join
    
    val goodStudentsRDD = goodStudentScoresDF.rdd.map(row => (row.getAs[String]("name"),row.getAs[Long]("score")))
                  .join(goodStudentInfoDF.rdd.map(
                      row => (row.getAs[String]("name"),row.getAs[Long]("age"))
                      ))
                      
                      
                      
    //然后将封装在RDD中的好学生的全部信息，转化为JavaRDD<Row> 的格式
                      
    val goodStudentRowRDD = goodStudentsRDD.map(
        t => Row(t._1,t._2._1.toInt,t._2._2.toInt))
    
    //将RDD转换为DataFrame
                      
    val structType = StructType(Array(
        StructField("name",StringType,true),
        StructField("score",IntegerType,true),
        StructField("age",IntegerType,true)))
        
        
    val goodStudentDF = sqlContext.createDataFrame(goodStudentRowRDD, structType)
    
    
    //最后将最好的学生的全部信息保存到一个JSON文件中
    //将DataFrame中的数据保存到外部的JSON文件中去
    
    goodStudentDF.write.json("C://Users//104515//Desktop//spark脚本数据//good_student_info1")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
}