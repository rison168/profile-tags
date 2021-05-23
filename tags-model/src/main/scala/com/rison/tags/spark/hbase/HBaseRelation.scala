package com.rison.tags.spark.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType

/**
 * @author : Rison 2021/5/21 下午2:39
 *自定义外部数据源：从HBase表加载数据和保存数据值HBase表
 */
case class HBaseRelation(context: SQLContext, params: Map[String, String], userSchema: StructType) extends BaseRelation with TableScan with InsertableRelation with Serializable {
  // 连接HBase数据库的属性名称
  val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
  val HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
  val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
  val HBASE_ZK_PORT_VALUE: String = "zkPort"
  val HBASE_TABLE: String = "hbaseTable"
  val HBASE_TABLE_FAMILY: String = "family"
  val SPERATOR: String = ","
  val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
  val HBASE_TABLE_ROWKEY_NAME: String = "rowKeyColumn"

  /**
   * SQLContext 实例对象
   * @return
   */
  override def sqlContext: SQLContext = context

  /**
   * DataFrame的Schema信息
   * @return
   */
  override def schema: StructType = userSchema

  /**
   * 如何从Hbase表中读取数据，返回RDD[Row]
   * @return
   */
  override def buildScan(): RDD[Row] = {
    //1 设置HBase中zookeeper集群信息
    val configuration = new Configuration()
    configuration.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
    configuration.set(HBASE_ZK_PORT_VALUE, params(HBASE_ZK_PORT_VALUE))

    //2 设置读HBase表的名称
    configuration.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE))

    //3 设置读取列簇和列名称
    val scan = new Scan()
    val familyBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
    scan.addFamily(familyBytes)
    val fields: Array[String] = params(HBASE_TABLE_SELECT_FIELDS).split(SPERATOR)
    fields.foreach(field => {
      scan.addColumn(familyBytes, Bytes.toBytes(field))
    })
    //过滤
//    configuration.set(
//      TableInputFormat.SCAN,
//      Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
//    )

    //4 调用底层API,读取HBase表数据
    val dataRDD: RDD[(ImmutableBytesWritable, Result)] = sqlContext.sparkContext.newAPIHadoopRDD(
      configuration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    val rowsRDD: RDD[Row] = dataRDD.map {
      case (_, result) => {
        val values: Array[String] = fields.map { field => Bytes.toString(result.getValue(familyBytes, Bytes.toBytes(field))) }
        Row.fromSeq(values)
      }
    }
    rowsRDD
  }
  /**
   * 将数据DataFrame写入到HBase表中
   * @param data 数据集
   * @param overwrite 保存模式
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    //1 数据转换
    val columns: Array[String] = data.columns
    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = data.rdd.map {
      row => {
        val rowKey: String = row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME))
        val put = new Put(Bytes.toBytes(rowKey))
        val familyBytes: Array[Byte] = Bytes.toBytes(params(HBASE_TABLE_FAMILY))
        columns.foreach(
          column => {
            put.addColumn(
              familyBytes,
              Bytes.toBytes(column),
              Bytes.toBytes(row.getAs[String](column))
            )
          }
        )
        (new ImmutableBytesWritable(put.getRow), put)
      }
    }

    //2 设置HBase中Zookeeper集群信息
    val conf = new Configuration()
    conf.set(HBASE_ZK_QUORUM_KEY, params(HBASE_ZK_QUORUM_VALUE))
    conf.set(HBASE_ZK_PORT_KEY, params(HBASE_ZK_PORT_VALUE))

    // 3 设置HBase表的名称
    conf.set(TableOutputFormat.OUTPUT_TABLE, params(HBASE_TABLE))

//    // 4. 保存数据到表
//    putsRDD.saveAsNewAPIHadoopFile(
//      "/apps/hbase/" + params(HBASE_TABLE) + "-" + System.currentTimeMillis(),
//      classOf[ImmutableBytesWritable],
//      classOf[Put],
//      classOf[TableOutputFormat[ImmutableBytesWritable]],
//      conf
//    )
  }
}
