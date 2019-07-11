package org.sparkutils

import java.util.Properties

import org.config.ConfigManager

object JDBCUtils {
  def getJdbcProp = {
    val prop = new Properties()
    prop.put("user",ConfigManager.getProper("jdbc.user"))
    prop.put("password",ConfigManager.getProper("jdbc.password"))
    prop.put("driver",ConfigManager.getProper("jdbc.driver"))
    val jdbcUrl = ConfigManager.getProper("jdbc.url")
    (prop,jdbcUrl)
  }
}
