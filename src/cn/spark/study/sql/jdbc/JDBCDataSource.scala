package cn.spark.study.sql.jdbc
import org.apache.spark.sql.SparkSession
import java.util.HashMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType}
/**
 * JDBC 数据源
 * GYJ
 * 2018-1-29
 */
object JDBCDataSource {
    val spark = SparkSession
                .builder()
                .appName("HiveDataResource")
                .config("spark.some.config.option","some-value")
                .getOrCreate()
    
                
    //总结一下：
		//jdbc数据源：
		//首先是通过:SparkSession的read系列方法， 加载MySQL中的数据加载为DataFrame；
		//然后可以将DataFrame转化为RDD，使用Spark core 提供的各种算子进行操作
		//最后可以将得到的数据结果，通过foreach()算子，写入mysql,hbase,redis等等db/cache中
		
		
		//分别将mysql中两张表的数据加载为DataFrame
          
    val studentInfosDF = spark.read.format("jdbc").options( 
         Map("url" -> "jdbc:mysql://spark1:3306/testdb",
             "dbtable" -> "student_infos")).load()
    
    val studentScoresDF = spark.read.format("jdbc").options( 
         Map("url" -> "jdbc:mysql://spark1:3306/testdb",
             "dbtable" -> "student_scores")).load()
    //将两个DataFrame 转换为JavaPairRDD，执行join操作
             
    val goodStudentsRDD = studentInfosDF.rdd.map(row => (row.getAs[String]("name"),row.getAs[Long]("age")))
                  .join(studentScoresDF.rdd.map(
                      row => (row.getAs[String]("name"),row.getAs[Long]("score"))
                      ))
                      
                      
                      
    //然后将封装在RDD中的好学生的全部信息，转化为JavaRDD<Row> 的格式
                      
    val goodStudentRowRDD = goodStudentsRDD.map(
        t => Row(t._1,t._2._1.toInt,t._2._2.toInt))
          
  
    
    //过滤出分数大于80分的数据
    val filteredRDD = goodStudentRowRDD.filter(t => t.getInt(3)> 80)
    
    //将JavaRDD转换为DataFrame
    
    val structType = StructType(Array(
        StructField("name",StringType,true),
        StructField("score",IntegerType,true),
        StructField("age",IntegerType,true)))
        
    val goodStudentDF = spark.createDataFrame(goodStudentRowRDD, structType)
    goodStudentDF.show()
    spark.stop()
}