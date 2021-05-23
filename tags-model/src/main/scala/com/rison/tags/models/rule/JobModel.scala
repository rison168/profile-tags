package com.rison.tags.models.rule

import com.rison.tags.meta.HBaseMeta
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @author : Rison 2021/5/23 上午9:20
 *         用户职业标签模型
 */
object JobModel {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result], classOf[Put]))
      .set("spark.sql.shuffle.partitions", "4")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().config("hive.metastore.uris", "thrift://bigdatacdh01.itcast.cn:9083")
      .config("spark.sql.warehouse.dir", "hdfs://bigdatacdh01.itcast.cn:8020/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //TODO : 1 依据tagId,从mysql读取标签数据
    val tagTable: String =
      """
        |(
        |SELECT `id`,
        | `name`,
        | `rule`,
        | `level`
        |FROM `profile_tags`.`tbl_basic_tag`
        |WHERE id = 321
        |UNION
        |SELECT `id`,
        | `name`,
        | `rule`,
        | `level`
        |FROM `profile_tags`.`tbl_basic_tag`
        |WHERE pid = 321
        |ORDER BY `level` ASC, `id` ASC
        |) AS basic_tag
        |""".stripMargin
    val basicTagDF: DataFrame = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://bigdata-cdh01.itcast.cn:3306/?useUnicode=true&characterEncoding=UTF8&serverTimezone=UTC")
      .option("dbtable", tagTable)
      .option("user", "root")
      .option("password", "123456")
      .load()
    basicTagDF.persist(StorageLevel.MEMORY_AND_DISK)

    //TODO： 2 解析标签业务规则获取业务数据
    val tagRule: String = basicTagDF.filter($"level" === 4)
      .head()
      .getAs[String]("rule")

    val ruleMap: Map[String, String] = tagRule.split("\\n")
      .map(
        line => {
          val Array(attrName, attrValue): Array[String] = line.trim.split("=")
          (attrName, attrValue)
        }
      )
      .toMap

    var businessDF: DataFrame = null

    if("hbase".equals(ruleMap("inType").toLowerCase)){
      //规则数据封装到hbaseMeta中
      val hBaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
      //依据条件到HBase表中获取业务数据
      businessDF = spark.read
        .format("hbase")
        .format("hbase")
        .option("zkHosts", hBaseMeta.zkHosts)
        .option("zkPort", hBaseMeta.zkPort)
        .option("hbaseTable", hBaseMeta.hbaseTable)
        .option("family", hBaseMeta.family)
        .option("selectFields", hBaseMeta.selectFieldNames)
        .load()
    }else{
      //如果获取不了数据，直接抛出异常
      new RuntimeException("业务标签未提供数据源信息，获取不到业务数据，无法计算标签")
    }

    businessDF.show(20, truncate = false)


    //TODO : 3 结合业务数据和属性标签数据，给用户打标签
    //获取5级标签对应的TagId 和 TagRule

    val attrTagRuleMap: Map[String, String] = basicTagDF
      .filter($"level" === 5)
      .select($"rule", $"name")
      .as[(String, String)]
      .rdd
      .collectAsMap().toMap

    val attrTagRuleMapBroadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(attrTagRuleMap)

    //自定义udf函数，依据Job职业和属性标签规则，进行标签化

    val job_to_tag: UserDefinedFunction = udf((job: String) => attrTagRuleMapBroadcast.value(job))
    val modelDF: DataFrame = businessDF.select($"id".as("userId"), job_to_tag($"job").as("job"))
    basicTagDF.unpersist()

    //TODO : 4 保存数据到HBase表中
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
