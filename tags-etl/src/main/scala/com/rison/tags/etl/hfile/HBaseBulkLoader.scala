package com.rison.tags.etl.hfile

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.parallel.{availableProcessors, immutable}

/**
 * @author : Rison 2021/5/21 上午9:49
 *将数据存储文件文件转换为HFile文件，加载到HBase表中
 */
object HBaseBulkLoader {
  def main(args: Array[String]): Unit = {

    //应用执行时传递5个参数： 数据类型、HBase表名称、表列簇、输入路径、输出路径
    /*
    args = Array("1", "tbl_tag_logs", "detail",
    "/user/hive/warehouse/tags_dat.db/tbl_logs",
    "/datas/output_hfile/tbl_tag_logs")
    args = Array("2", "tbl_tag_goods", "detail",
    "/user/hive/warehouse/tags_dat.db/tbl_goods",
    "/datas/output_hfile/tbl_tag_goods")
    args = Array("3", "tbl_tag_users", "detail",
    "/user/hive/warehouse/tags_dat.db/tbl_users",
    "/datas/output_hfile/tbl_tag_users")
    args = Array("4", "tbl_tag_orders", "detail",
    "/user/hive/warehouse/tags_dat.db/tbl_orders",
    "/datas/output_hfile/tbl_tag_orders")
    */

    if(args.length != 5){
      println("Usage: required params: <DataType> <HBaseTable><Family> <InputDir> <OutputDir>")
      System.exit(-1)
        }
    // 将传递赋值给变量， 其中数据类型：1Log、2Good、3User、4Order
    val Array(dataType, tableName, family, inputDir, outputDir) = args

    // 依据参数或俺去处理的schema
    val fieldNames: TreeMap[String, Int] = dataType.toInt match {
      case 1 => TableFileNames.LOG_FIELD_NAMES
      case 2 => TableFileNames.GOODS_FIELD_NAMES
      case 3 => TableFileNames.USER_FIELD_NAMES
      case 4 => TableFileNames.ORDER_FIELD_NAMES
    }

    // 1 构建SparkContext实例对象
    //a.创建SparkConf,设置应用配置信息
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .set("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
    //b.创建SparkContext
    val sc: SparkContext = SparkContext.getOrCreate(sparkConf)

    //2 读取文件文本数据，转换格式
    val keyValuesRDD: RDD[(ImmutableBytesWritable, KeyValue)] = sc.textFile(inputDir)
      .filter(line => null != line)
      .flatMap(line => getLineToDate(line, family, fieldNames))
      .sortByKey()

    // TODO : 构建Job,设置相关配置信息，主要为输出格式
    //a 读取配置信息
    val conf: Configuration = HBaseConfiguration.create()
    //b 如果输出存在目录，删除
    val dfs: FileSystem = FileSystem.get(conf)
    val outPutPath = new Path(outputDir)
    if (dfs.exists(outPutPath)){
      dfs.delete(outPutPath, true)
    }
    dfs.close()

    //TODO :  配置HFileOutPutFormat2输出
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val hTableName: TableName = TableName.valueOf(tableName)
    val table: Table = conn.getTable(hTableName)
    HFileOutputFormat2.configureIncrementalLoad(
      Job.getInstance(conf),
      table,
      conn.getRegionLocator(hTableName)
    )

    // TODO： 保存数据为HFile文件
    keyValuesRDD.saveAsNewAPIHadoopFile(
      outputDir, //
      classOf[ImmutableBytesWritable], //
      classOf[KeyValue], //
      classOf[HFileOutputFormat2], //
      conf //
    )

    //TODO : 将输出到HFile加载到HBase表中
    val load = new LoadIncrementalHFiles(conf)
    load.doBulkLoad(outPutPath, conn.getAdmin, table, conn.getRegionLocator(hTableName))
    //关闭应用
    sc.stop()


  }
  def getLineToDate(line: String, family: String, filedNames: TreeMap[String, Int]) = {
    val length: Int = filedNames.size
    //分割字符串
    val fieldValues: Array[String] = line.split("\\t", -1)
    if (null == fieldValues || fieldValues.length != length) Nil

    //获取id,构建RowKey
    val id: String = fieldValues(0)
    val rowKey: Array[Byte] = Bytes.toBytes(id)
    val ibw = new ImmutableBytesWritable(rowKey)

    //列簇
    val columnFamily: Array[Byte] = Bytes.toBytes(family)

    //构建keyValue对象
    filedNames.toList.map{
      case(fileName, fieldIndex) =>
        val keyValue = new KeyValue(
          rowKey,
          columnFamily,
          Bytes.toBytes(fileName),
          Bytes.toBytes(fieldIndex)
        )
        (ibw, keyValue)
    }
  }


}
