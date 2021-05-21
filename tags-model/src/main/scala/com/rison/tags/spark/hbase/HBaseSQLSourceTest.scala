package com.rison.tags.spark.hbase

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author : Rison 2021/5/21 下午4:17
 *测试自定义外部数据源实现从HBase表读写数据接口
 */
object HBaseSQLSourceTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[*]")
      .config("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //读取数据
    val usersDF: DataFrame = spark.read
      .format("hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_tag_users")
      .option("family", "detail")
      .option("selectFields", "id,gender")
      .load()

    usersDF.printSchema()
    usersDF.cache()
    usersDF.show(50, truncate = false)

    // 保存数据
    usersDF.write
      .mode(SaveMode.Overwrite)
      .format("hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_users")
      .option("family", "info")
      .option("rowKeyColumn", "id")
      .save()
    spark.stop()

  }

}
