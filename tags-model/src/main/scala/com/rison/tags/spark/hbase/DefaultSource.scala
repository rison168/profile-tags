package com.rison.tags.spark.hbase

import org.apache.spark.sql.catalyst.expressions.CurrentRow.nullable
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @author : Rison 2021/5/21 下午4:02
 *自定义外部数据源HBase，提供BaseRelation对象，用于加载数据和保存数据
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister with Serializable {
  // 参数信息
  val HBASE_TABLE_SELECT_FIELDS: String = "selectFields"
  val SPERATOR: String = ","

  /**
   * 返回BaseRelation实例对象，提供加载数据功能
   *
   * @param sqlContext SQLContext实例对象
   * @param parameters 参数信息
   * @return
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    //定义schema 信息
    val schema: StructType = StructType(
      parameters(HBASE_TABLE_SELECT_FIELDS)
        .split(SPERATOR)
        .map{field =>
          StructField(field, StringType, nullable = true)
        }
    )
    // 2. 创建HBaseRelation对象
    val relation = new HBaseRelation(sqlContext, parameters, schema)
    // 3. 返回对象
    relation
  }
  /**
   * 返回BaseRelation实例对象，提供保存数据功能
   *
   * @param sqlContext SQLContext实例对象
   * @param mode 保存模式
   * @param parameters 参数
   * @param data 数据集
   * @return
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    // 1. 创建HBaseRelation对象
    val relation = new HBaseRelation(sqlContext, parameters,
      data.schema)
    // 2. 插入数据
    relation.insert(data, overwrite = true)
    // 3. 返回对象
    relation

  }

  override def shortName(): String = "habse"
}
