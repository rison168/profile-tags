package com.rison.tags.meta

import com.rison.tags.util.DateUtils

/**
 * @author : Rison 2021/5/22 下午3:48
 *
 */
/**
 *
 * @param zkHosts
 * @param zkPort
 * @param hbaseTable
 * @param family
 * @param selectFieldNames
 */
case class HBaseMeta(
                      zkHosts: String,
                      zkPort: String,
                      hbaseTable: String,
                      family: String,
                      selectFieldNames: String,
                      filterConditions: String
                    )


object HBaseMeta {
  /**
   * 将Map集合数据解析到HBaseMeta中
   *
   * @param ruleMap map集合
   * @return
   */
  def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {
    // TODO: 实际开发中，应该先判断各个字段是否有值，没有值直接给出提示，终止程序运行，此处省略
    // a. 获取条件字段的值
    val whereCondition: String = ruleMap.getOrElse("whereCondition", null)
    // b. 解析条件字段的值，构建WHERE CAUSE语句
    /*
    whereCondition=modified#day#30
    whereCondition=modified#month#6
    whereCondition=modified#year#1
    */
    val conditionStr: String = if (null != whereCondition) {
      // i. 字符串分割，获取字段名、单位：天、月、年及数字
      val Array(field, unit, amount) = whereCondition.split("#")
      // ii. 获取昨日日期
      val nowDate: String =
        DateUtils.getNow(DateUtils.SHORT_DATE_FORMAT)
      val yesterdayDate: String = DateUtils.dateCalculate(nowDate,
        -1)
      // iii. 依据传递的单位unit，获取最早日期时间
      val agoDate: String = unit match {
        case "day" =>
          DateUtils.dateCalculate(yesterdayDate, -amount.toInt)
        case "month" =>
          DateUtils.dateCalculate(yesterdayDate, -(amount.toInt
            * 30))
        case "year" =>
          DateUtils.dateCalculate(yesterdayDate, -(amount.toInt
            * 365))
      }
      // iv. 构建WHERE CAUSE语句
      s"$field[GE]$agoDate,$field[LE]$yesterdayDate"
    } else null

      // TODO: 实际开发中，应该先判断各个字段是否有值，没有值直接给出提示，终止程序运行，此处省略
      HBaseMeta(
        ruleMap("zkHosts"),
        ruleMap("zkPort"),
        ruleMap("hbaseTable"),
        ruleMap("family"),
        ruleMap("selectFieldNames"),
        conditionStr
      )
    }

}