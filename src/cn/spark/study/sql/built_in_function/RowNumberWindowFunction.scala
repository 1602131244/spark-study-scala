package cn.spark.study.sql.built_in_function
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.hive.HiveContext
/**
 * 开窗函数以及top3销售额统计案例
 * GYJ
 * 2018-1-29
 */
object RowNumberWindowFunction {
  
  def main (args:Array[String]) : Unit ={
    val conf = new SparkConf()
                   .setMaster("local")
                   .setAppName("RowNumberWindowFunction")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    //创建销售额表，sales
    
    hiveContext.sql("DROP TABLE IF EXISTS sales")
    hiveContext.sql("CREATE TABLE IF NOT EXISTS sales ("
				+"product STRING,"
				+"category STRING,"
				+"revenue BIGINT)")
	  hiveContext.sql("LOAD DATA "
				+"LOCAL INPATH '/opt/modules/spark-study/resource/sales.txt' "
				+ "INTO TABLE sales")
				
				
	  // 开始编写我们的统计逻辑，使用row_number()开窗函数  
    // 先说明一下，row_number()开窗函数的作用  
    // 其实，就是给每个分组的数所在，按照其排序顺序，打上一个分组内的行号  
    // 比如说，有一个分组date=20151001, 里面有3条数据，1122，1121，1124，  
    // 那么对这个分组的每一行使用row_number()开窗函数以后，三行，依次会获得一个组内的行号  
    // 行号从1开始递增，比如1122 1， 1121 2， 1124， 3 
				
		val top3SaleDF = hiveContext.sql(""  
                + "SELECT product, category,revenue "  
                + "FROM ("  
                    + "SELECT "  
                        + "product, "  
                        + "category, "  
                        + "revenue, "  
                        // row_number()开窗函数的语法说明  
                        // 首先可以，在SELECT查询时，使用row_number()函数  
                        // 其次，row_number()函数后面先跟上OVER关键字  
                        // 然后括号中，是PARTITION BY，也就是说根据哪个字段进行分组  
                        // 其次是可以用ORDER BY 进行组内排序  
                        // 然后row_number()就可以给每个组内的行，一个组内行号  
                        + "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "  
                        + "FROM sales "  
                + ") tmp_sales "  
                + "WHERE rank<=3");  
        // 将每组排名前3的数据，保存到一个表中  
        hiveContext.sql("DROP TABLE IF EXISTS top3_sales");  
        top3SaleDF.write.saveAsTable("top3_sales");  
            
  }
}