package com.rison.tags.meta

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
                      selectFieldNames: String
                    )


object HBaseMeta{
  /**
   * 将Map集合数据解析到HBaseMeta中
   * @param ruleMap map集合
   * @return
   */
  def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {
    // TODO: 实际开发中，应该先判断各个字段是否有值，没有值直接给出提示，终止程序运行，此处省略
    HBaseMeta(
      ruleMap("zkHosts"),
      ruleMap("zkPort"),
      ruleMap("hbaseTable"),
      ruleMap("family"),
      ruleMap("selectFieldNames")
    )
  }
}