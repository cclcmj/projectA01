package org.spark_dw

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.config.ConfigManager
import org.constants.Constan
import org.slf4j.LoggerFactory
import org.sparkutils.JDBCUtils

object DWD_DWS {
  def main(args: Array[String]): Unit = {
    val hc = SparkSession.builder().enableHiveSupport().appName(Constan.SPARK_APP_NAME_USER).master(Constan.SPARK_LOCAL).getOrCreate()
//    val sql = ConfigManager.getProper(args(0))
//    if(sql == null) {
//      LoggerFactory.getLogger("SparkLogger").debug("提交的表名参数有问题")
//    }else{
//      val finalSql = sql.replace("?",args(1))
//      val df = hc.sql(finalSql)
//      val mysqlTableTame = args(0).split("\\.")(1)
//      val hiveTableName = args(0)
//      val jdbcProp = JDBCUtils.getJdbcProp._1
//      val jdbcUrl = JDBCUtils.getJdbcProp._2
//      //df.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl,mysqlTableTame,jdbcProp)
//      df.write.mode(SaveMode.Append).format("HIVE").partitionBy("dt").saveAsTable(hiveTableName)
    val sql = ConfigManager.getProper("user_basic")
    val df = hc.sql(sql)
    val hiveTableName = "qfbap_dm.dm_user_basic"
    val mysqlTableName = "dm_user_basic"
    val jdbcProp = JDBCUtils.getJdbcProp._1
    val jdbcUrl = JDBCUtils.getJdbcProp._2
    df.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl,mysqlTableName,jdbcProp)
//    df.write.mode(SaveMode.Append).insertInto(hiveTableName)
    }
//  }
}
