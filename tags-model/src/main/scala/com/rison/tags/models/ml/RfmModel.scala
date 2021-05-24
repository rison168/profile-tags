package com.rison.tags.models.ml

import breeze.linalg
import com.rison.tags.models.{AbstractModel, ModelType}
import org.apache.spark.ml
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @author : Rison 2021/5/24 上午10:23
 *         标签模型开发， 客户价值rfm模型
 */
class RfmModel extends AbstractModel("客户价值RFM模型", ModelType.ML) {
  /**
   *
   * 361 客户价值
   * 362 高价值 0
   * 363 中上价值 1
   * 364 中价值 2
   * 365 中下价值 3
   * 366 低价值 4
   *
   * @param businessDF
   * @param tagDF
   * @return
   */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val session: SparkSession = businessDF.sparkSession
    import session.implicits._
    import org.apache.spark.sql.functions._
    // 1. 获取属性标签（5级标签）数据，选择id,rule
    val rulesMap: Map[String, Long] = TagTools.convertMap(tagDF)
    /*
    (4,高价值)
    (1,中上价值)
    (0,中价值)
    (2,中下价值)
    (3,超低价值)
    */
    rulesMap.foreach(println)
    // 2. 从业务数据中计算R、F、M值
    val rfmDF: DataFrame = businessDF
      .groupBy($"memberid") // 按照用户分组
      // 使用函数计算R、F、M值
      .agg(
        max($"finishtime").as("last_time"), //
        // 计算F值
        count($"ordersn").as("frequency"), //
        // 计算M值
        sum(
          $"orderamount".cast(
            DataTypes.createDecimalType(10, 2))
        ).as("monetary")
      )
      .select(
        $"memberid".as("uid"), //
        // 计算R值
        datediff(
          current_timestamp(), from_unixtime($"last_time") //
        ).as("recency"), //
        $"frequency", $"monetary"
      )

    // 3. 按照规则，给RFM值打分: Score
    /*
    R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
    F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
    M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
    */
    // R 打分条件表达式
    val rWhen = when(col("recency").between(1, 3), 5.0) //
      .when(col("recency").between(4, 6), 4.0) //
      .when(col("recency").between(7, 9), 3.0) //
      .when(col("recency").between(10, 15), 2.0) //
      .when(col("recency").geq(16), 1.0) //
    // F 打分条件表达式
    val fWhen = when(col("frequency").between(1, 49), 1.0) //
      .when(col("frequency").between(50, 99), 2.0) //
      .when(col("frequency").between(100, 149), 3.0) //
      .when(col("frequency").between(150, 199), 4.0) //
      .when(col("frequency").geq(200), 5.0) //
    // M 打分条件表达式
    val mWhen = when(col("monetary").lt(10000), 1.0) //
      .when(col("monetary").between(10000, 49999), 2.0) //
      .when(col("monetary").between(50000, 99999), 3.0) //
      .when(col("monetary").between(100000, 199999), 4.0) //
      .when(col("monetary").geq(200000), 5.0) //
    val rfmScoreDF: DataFrame = rfmDF.select(
      $"uid", //
      rWhen.as("r_score"), //
      fWhen.as("f_score"), //
      mWhen.as("m_score") //
    )

    // 4. 组合R\F\M列为特征值features
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("features")
    val rfmFeaturesDF: DataFrame = assembler.transform(rfmScoreDF)
    // 算法使用数据集DataFrame变量名称：featuresDF
    val featuresDF: DataFrame = rfmFeaturesDF.withColumnRenamed("raw_features", "features")
    // 将训练数据缓存
    featuresDF.persist(StorageLevel.MEMORY_AND_DISK)
    // 5. 使用KMeans聚类算法模型训练
    val kMeansModel: KMeansModel = new KMeans()
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setK(5) // 设置列簇个数：5
      .setMaxIter(10) // 设置最大迭代次数
      .fit(featuresDF)
    println(s"WSSSE = ${kMeansModel.computeCost(featuresDF)}")
    // 6. 使用模型预测
    val predictionDF: DataFrame = kMeansModel.transform(featuresDF)
    featuresDF.unpersist()

    // 7. 查看各个类簇中，RFM之和的最大和最小值
    val clusterDF: DataFrame = predictionDF
      .select(
        $"prediction", //
        ($"r_score" + $"f_score" + $"m_score").as("rfm_score") //
      )
      .groupBy($"prediction")
      .agg(
        max($"rfm_score").as("max_rfm"), //
        min($"rfm_score").as("min_rfm") //
      )
    //clusterDF.show(10, truncate = false)

    // 8. 获取聚类模型中簇中心及索引
    val clusterCenters: Array[ml.linalg.Vector] =
      kMeansModel.clusterCenters
    val centerIndexArray: Array[((Int, Double), Int)] = clusterCenters
      .zipWithIndex
      .map{case(vector, centerIndex) => (centerIndex,
        vector.toArray.sum)}
      .sortBy{case(_, rfm) => - rfm}
      .zipWithIndex
    //centerIndexArray.foreach(println)
    // 9. 聚类类簇关联属性标签数据rule，对应聚类类簇与标签tagId
    val indexTagMap: Map[Int, String] = centerIndexArray
      .map{case((centerIndex, _), index) =>
        val tagName = rulesMap(index.toString)
        (centerIndex, tagName)
      }
      .toMap
    //indexTagMap.foreach(println)
    // 10. 关联聚类预测值，将prediction转换为tagId
    val indexTagMapBroadcast =
    session.sparkContext.broadcast(indexTagMap)
    val index_to_tag = udf(
      (prediction: Int) => indexTagMapBroadcast.value(prediction)
    )
    val modelDF: DataFrame = predictionDF
      .select(
        $"userId", //
        index_to_tag($"prediction").as("rfm") //
      )
    //modelDF.show(100, truncate = false)
    // 返回标签数据
    modelDF
  }

}

object RfmModel{
  def main(args: Array[String]): Unit = {
    val tagModel = new RfmModel()
    tagModel.executeModel(361L)
  }
}
