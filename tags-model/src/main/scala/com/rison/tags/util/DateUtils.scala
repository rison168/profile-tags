package com.rison.tags.util

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat



/**
 * @author : Rison 2021/5/24 上午10:04
 *
 */
/**
 * 日期时间工具类
 */
object DateUtils {
  // 格式：年－月－日 小时：分钟：秒
  val LONG_DATE_FORMAT: String = "yyyy-MM-dd HH:mm:ss"
  // 格式：年－月－日
  val SHORT_DATE_FORMAT: String = "yyyy-MM-dd"

  /**
   * 获得当前日期字符串，默认格式"yyyy-MM-dd HH:mm:ss"
   *
   * @return
   */
  def getNow(format: String = SHORT_DATE_FORMAT): String = {
    // 获取当前Calendar对象
    val today: Calendar = Calendar.getInstance()
    // 返回字符串

    dateToString(today.getTime, format)
  }

  /**
   * 依据日期时间增加或减少多少天
   *
   * @param dateStr 日期字符串
   * @param amount  天数，可以为正数，也可以为负数
   * @param format  格式，默认为：yyyy-MM-dd HH:mm:ss
   * @return
   */
  def dateCalculate(dateStr: String, amount: Int,
                    format: String = SHORT_DATE_FORMAT): String = {
    // a. 将日期字符串转换为Date类型
    val date: Date = stringToDate(dateStr, format)
    // b. 获取Calendar对象
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    // c. 设置天数（增加或减少）
    calendar.add(Calendar.DAY_OF_YEAR, amount)
    // d. 转换Date为字符串
    dateToString(calendar.getTime, format)
  }

  /**
   * 把日期转换为字符串
   *
   * @param date   日期
   * @param format 格式
   * @return
   */
  def dateToString(date: Date, format: String): String = {
    // a. 构建FastDateFormat对象
    val formatter: FastDateFormat = FastDateFormat.getInstance(format)
    // b. 转换格式
    formatter.format(date)
  }

  /**
   * 把日期格式的字符串转换为日期Date类型
   *
   * @param dateStr 日期格式字符串
   * @param format  格式
   * @return
   */
  def stringToDate(dateStr: String, format: String): Date = {
    // a. 构建FastDateFormat对象
    val formatter: FastDateFormat = FastDateFormat.getInstance(format)
    // b. 转换格式
    formatter.parse(dateStr)
  }
}

