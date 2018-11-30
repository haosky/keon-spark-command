
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale, Properties}
import com.mongodb.BasicDBObject
import com.mongodb.client.model.UpdateOptions
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.bson.Document
import org.joda.time.{DateTime, Days}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import scala.collection.mutable
import scala.reflect.ClassTag
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging


/**
  * Created by Keon on 2018/9/11
  *实用的工具
  ***/
package object common {

  // 获取 根据当天偏移日
  def DateNumOffset(num: Int): Date = {
    val cal = Calendar.getInstance
    val now = new Date
    cal.setTime(now)
    cal.add(Calendar.DATE, num)
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
    cal.getTime
  }

  // 获取 根据当天偏移日
  def DateNumOffset(DateStr: String, num: Int): Date = {
    val cal = Calendar.getInstance
    val now = StringToDate(DateStr)
    cal.setTime(now)
    cal.add(Calendar.DATE, num)
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
    cal.getTime
  }

  // 获取 根据当天偏移日，与mongodb 时区有关
  def MongodbDateNumOffset(num: Int): Date = {
    val cal = Calendar.getInstance
    val now = new Date
    cal.setTime(now)
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
    val sdfz = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z", Locale.US)
    cal.add(Calendar.DATE, num)
    sdfz.parse(sdf.format(cal.getTime) + " UTC")
  }

  def StringToDateTime(strDate: String): DateTime = {
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val dt = formatter.parseDateTime(strDate)
    dt
  }

  def StringToDate(strDate: String): Date = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    val dt = formatter.parse(strDate)
    dt
  }

  def DateToString(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)
    sdf.format(date)
  }

  def DateFromWeekOfStart(dateStr: String): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    val d = sdf.parse(dateStr)
    cal.setTime(d)
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    var day = cal.get(Calendar.DAY_OF_WEEK)
    day = day - 1
    println(day)
    if (day == 0) day = 7
    println(day)
    cal.add(Calendar.DATE, -day + 1)
    sdf.format(cal.getTime)
  }

  // 获取日期的前几天
  def DaysBefore(dt: Date, interval: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt)
    cal.add(Calendar.DATE, -interval)
    val before = dateFormat.format(cal.getTime())
    before
  }

  def LocalMysqlSettings(): (String, String, String) = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("localmysql.properties")
    prop.load(inputStream)
    (prop.getProperty("user"), prop.getProperty("passwd"), prop.getProperty("url"))
  }

  def RemoteMongoSettings(): Properties = {
    val prop = new Properties()
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("remotemongo.properties")
    prop.load(inputStream)
    prop
  }

  // 计算时间区间，并分群
  def CalcDateInterval(dt1: String, dt2: String, interval: Int): Int = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss")
    val diffInDays = Days.daysBetween(DateTime.parse(dt1, dateFormat), DateTime.parse(dt2, dateFormat))
    val days = diffInDays.getDays
    days - days % interval
  }

  def MysqlProperites(): Properties = {
    val connectionProperties = new Properties()
    val (user, passwd, url) = LocalMysqlSettings()
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    connectionProperties.put("user", user)
    connectionProperties.put("password", passwd)
    connectionProperties.put("url", url)
    connectionProperties
  }

  def arrayToStr[T](data: mutable.WrappedArray[T]): String = {
    if (data != null) {
      data.mkString(",")
    }
    else
      ""
  }

  // 获取某个日期所在周的第一天
  def DaysWeekBefore(strDate: String): String = {
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    val dt = formatter.parse(strDate)
    cal.setTime(dt)
    cal.add(Calendar.DATE, -7)
    val before = formatter.format(cal.getTime())
    before
  }

  /**
    * 以闰年为准，生成一年的所有天数
    *
    * @return 字符串数组
    */
  def makeYearDaySeq(start: String = "2020-01-01", end: String = "2020-12-31"): Array[String] = {
    val timeStart = new DateTime(start)
    val timeEnd = new DateTime(end)
    val daysCount = Days.daysBetween(timeStart, timeEnd).getDays()
    val timeSeq = (0 to daysCount).map(timeStart.plusDays(_))
    timeSeq.map(x => "%02d-%02d".format(x.getMonthOfYear, x.getDayOfMonth)).toArray
  }

  /**
    * 根据起止日期，生成中间的所有天数，接收的格式是 yyyy-mm-dd
    *
    * @param start 开始日期
    * @param end   结束日期
    * @return 字符串数组
    */
  def makeYearDaySeqByStartEnd(start: String, end: String): Array[String] = {
    val timeStart = new DateTime(start)
    val timeEnd = new DateTime(end)
    val daysCount = Days.daysBetween(timeStart, timeEnd).getDays()
    val timeSeq = (0 to daysCount).map(timeStart.plusDays(_))
    timeSeq.map(x => x.toString().slice(0, 10)).toArray
  }

  object StreamingLog extends Logging {

    /** Set reasonable logging levels for streaming if the user has not configured log4j. */
    def setStreamingLogLevels() {
      val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
      if (!log4jInitialized) {
        // We first log something to initialize Spark's default logging, then we override the
        // logging level.
        logInfo("Setting log level to [WARN] for streaming example." +
          " To override add a custom log4j.properties to the classpath.")
        Logger.getRootLogger.setLevel(Level.INFO)
      }
    }
  }

}

