package tools

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.io.{Closeable, Serializable}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
/**
  * Created by Keon on 2018/11/30
  * mysql 插入、 更新 写入工具
  */
object MySqlWriter{

  def  MySqlSparkUpsert[D: ClassTag](rdd: RDD[D], writeConfig: SparkConf,fun:(D,PreparedStatement) => Unit): Unit = {

    rdd.foreachPartition( iter =>  if (iter.nonEmpty) {
        MySQLConn(writeConfig).withMySqlClientDo( c =>{
          iter.foreach(x=>fun(x,c))
          val euReturnValue = c.executeUpdate()
          if(c !=null && c.isClosed) c.close()
        })
    })
  }

  case class MySQLConn(writeConfig: SparkConf) extends Logging with Serializable with Closeable{
    var conn: Connection = _
    def withMySqlClientDo[T](ps: PreparedStatement => T): T = {
      try {
        val stat = buildStatement(writeConfig)
        ps(stat)
      } finally {
        close()
      }
    }
    def releaseClient(ps: PreparedStatement): Unit ={
      if(ps !=null && ps.isClosed) ps.close()
    }

    def buildStatement(writeConfig: SparkConf):PreparedStatement = {
      if(conn ==null || conn.isClosed)
        {
          conn = DriverManager.getConnection(
            writeConfig.get("spark.mysql.url"),
            writeConfig.get("spark.mysql.user"),
            writeConfig.get("spark.mysql.password"))
        }
      conn.prepareStatement(writeConfig.get("spark.mysql.sql")) // INSERT INTO customers (name, email) " + "VALUES (?, ?) " + "ON DUPLICATE KEY UPDATE " + "name = VALUES(name), " + "id = LAST_INSERT_ID(id)
    }

    override def close(): Unit =  {
      if(conn != null && !conn.isClosed)  conn.close()
    }
  }

}


