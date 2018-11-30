import common.LocalMysqlSettings
import org.apache.spark.sql.SparkSession
import tools.MySqlWriter

/**
  * Created by Keon on 2018/11/30
  * mysql 按主键更新的spark案例
  */
object MysqlUpsertExample {

  def main(args:Array[String]): Unit ={
    val spark = SparkSession
      .builder()
      .appName("MysqlUpsertExample")
      .master("local[*]")
      .getOrCreate()

    val table = "tttest111"
    val mysqlDatabaseName = "icecastle"
    val (user,passwd,url_bi)  = LocalMysqlSettings()
    val url = s"jdbc:mysql://10.200.102.197:3306/$mysqlDatabaseName?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true"

    val rddz = spark.sparkContext.parallelize(Seq((1,"a3","1"),(2,"bzzz","1"),(3,"a43","2"),(4,"a","634")))

//    import org.apache.spark.sql.functions.monotonically_increasing_id
//    rddz.toDF("id","c1","c2").write.format("jdbc")
//      .option("url", url)
//      .option("dbtable", "tttest111")
//      .option("user", user)
//      .option("password", passwd)
//      .option( "driver" , "com.mysql.jdbc.Driver")
//      .mode(SaveMode.Overwrite)
//      .save()
//
    // TODO
    // 人工设置主键

    // 更新操作
    val sql =  s"INSERT INTO $table (id,c1, c2) " + "VALUES (?,?, ?) " + "ON DUPLICATE KEY UPDATE " +  "id = ?,c1=?,c2=?"
    val resconfig =  spark.sparkContext.getConf.set("spark.mysql.url",url).set("spark.mysql.password",passwd).set("spark.mysql.user",user).set("spark.mysql.sql",sql)
    MySqlWriter.MySqlSparkUpsert[(Int,String,String)](rddz,resconfig,(unit,ps) => {
      ps.setInt(1,unit._1)
      ps.setInt(4,unit._1)
      ps.setString(2,unit._2)
      ps.setString(5,unit._2)
      ps.setString(3,unit._3)
      ps.setString(6,unit._3)
    })
    spark.stop()
  }
}