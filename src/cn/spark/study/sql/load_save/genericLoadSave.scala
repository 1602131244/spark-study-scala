package cn.spark.study.sql.load_save
import org.apache.spark.sql.SparkSession
/**
 * lOAD AND save 的parquet文件操作
 * GYJ
 * 2018-1-29
 */
object genericLoadSave {
  def main(args:Array[String]) : Unit= {
    val spark = SparkSession
                .builder
                .master("local")
                .appName("genericLoadSave")
                .config("spark.some.config.option","some-value")
                .getOrCreate()
    val usersDF = spark.read.load("C://Users//104515//Desktop//spark脚本数据//users.parquet")
    usersDF.printSchema()
    usersDF.show()
    usersDF.select("name", "favorite_color").write.save("C://Users//104515//Desktop//spark脚本数据//nameAndColors.parquet")
  }
}