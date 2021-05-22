package com.rison.tags.models.rule

import com.rison.tags.meta.HBaseMeta
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel


/**
 * @author : Rison 2021/5/22 下午3:05
 *用户性别标签模型
 */
object GenderModel extends Logging{
  def main(args: Array[String]): Unit = {
    // TODO : 1、创建SparkSession实例对象
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]").set("spark.sql.shuffle.partitions", "4")
      .set("spark.serializer",
      "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result], classOf[Put]))
//    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //TODO : 2、 从MySql数据库读取标签的数据，依据业务标签ID读取 tbl_basic_tag

    /**
     * 318 性别 4
     * 319 男=1 5
     * 320 女=2 5
     */

    val tagTable: String =
      """
        |(
        |SELECT `id`,
        |`name`,
        |`rule`,
        |`level`
        |FROM `profile_tags`.`tbl_basic_tag`
        |WHERE id = 318
        |UNION
        |SELECT `id`,
        |`name`,
        |`rule`,
        |`level`
        |FROM `profile_tags`.`tbl_basic_tag`
        |WHERE pid = 318
        |ORDER BY `level` ASC, `id` ASC
        |) AS basic_tag
        |
        |""".stripMargin
    val basicTagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?" +
        "useUnicode=true&characterEncoding=UTF8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("user", "root")
      .option("password", "123456")
      .load()
    basicTagDF.persist(StorageLevel.MEMORY_AND_DISK)

    /**
     * root
     * |-- id: long (nullable = false)
     * |-- name: string (nullable = true)
     * |-- rule: string (nullable = true) inType=hbase\nzkHosts=bigdatacdh01.itcast.cn\nzkPort=2181\nhbaseTable=tbl_tag_users\nfamily=detail\nselec
     * tFieldNames=id,gender
     * |-- level: integer (nullable = true)
     */

    //TODO : 3、 业务标签规则获取业务数据，比如HBase数据库读取表数据
    // 4级标签规则rule
    val tagRule: String = basicTagDF
      .filter($"level" === 4)
      .head()
      .getAs[String]("rule")
    logInfo(s"============业务标签数据规则================{$tagRule}")
    // 解析标签规则，先按照\n分割，再按照等号分割
    /**
     * inType=hbase
     * zkHosts=bigdata-cdh01.itcast.cn
     * zkPort=2181
     * hbaseTable=tbl_tag_users
     * family=detail
     * selectFieldNames=id,gender
     */
    val ruleMap: Map[String, String] = tagRule.split("\n")
      .map(
        line => {
          val Array(attrName, attrValue) = line.trim.split("=")
          (attrName, attrValue)
        }
      )
      .toMap
    //依据标签规则中inType类型获取数据
    var businessDF: DataFrame = null
    if ("hbase".equals(ruleMap("inType").toLowerCase())){
      //规则数据封装到HBaseMeta中
      val hBaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      // 依据条件到HBase中获取业务数据
      businessDF = spark.read
        .format("hbase")
        .option("zkHosts", hBaseMeta.zkHosts)
        .option("zkPort", hBaseMeta.zkPort)
        .option("hbaseTable", hBaseMeta.hbaseTable)
        .option("family", hBaseMeta.family)
        .option("selectFields", hBaseMeta.selectFieldNames)
        .load()
    }else{
      // 如果未获取到数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }
    businessDF.printSchema()
    businessDF.show(20, truncate = false)

    //TODO : 4、 业务表数据和属性标签结合，构建标签：规则匹配标签

    // 获取5级标签对应的tagRule和tagName
    val attrTagRuleDF: DataFrame = basicTagDF.filter($"level" === 5).select($"rule", $"name")
    // DataFrame 关联，依据属性标签规则rule与 业务数据字段gender
    val modelDF: DataFrame = businessDF.join(attrTagRuleDF, businessDF("gender") === attrTagRuleDF("rule"))
      .select($"id".as("userId"), $"name".as("gender"))
    basicTagDF.unpersist()

    /**
     * root
     * |-- userId: string (nullable = true)
     * |-- gender: string (nullable = true)
     */
    //TODO : 5、 将标签数据存储到HBase表中，用户画像标签表 tbl_profile
    // 保存数据
    modelDF.write
      .mode(SaveMode.Overwrite)
      .format("hbase")
      .option("zkHosts", "bigdata-cdh01.itcast.cn")
      .option("zkPort", "2181")
      .option("hbaseTable", "tbl_users")
      .option("family", "info")
      .option("rowKeyColumn", "userId")
      .save()
    spark.stop()
  }

}
