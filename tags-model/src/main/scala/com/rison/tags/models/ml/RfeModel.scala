package com.rison.tags.models.ml

import com.rison.tags.models.{AbstractModel, ModelType}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Column, DataFrame, SparkSession}



/**
 * @author : Rison 2021/5/25 上午9:41
 *         开发标签模型（挖掘类型标签）：用户活跃度RFE模型
 *         Recency: 最近一次访问时间,用户最后一次访问距今时间
 *         Frequency: 访问频率,用户一段时间内访问的页面总次数,
 *         Engagements: 页面互动度,用户一段时间内访问的独立页面数,也可以定义为页面 浏览
 *         量、下载量、 视频播放数量等
 */
class RfeModel extends AbstractModel("用户活跃度模型RFE模型", ModelType.ML){
  /*
367 用户活跃度
368 非常活跃 0
369 活跃 1
370 不活跃 2
371 非常不活跃 3
*/
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
    val spark: SparkSession = businessDF.sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //  获取属性标签（5级标签）数据，选择id,rule
    val rulesMap: Map[String, Long] = TagTools.convertMap(tagDF)
    //1 从业务数据中计算RFE的值
    val rfeDF: DataFrame = businessDF
      .groupBy($"global_user_id") //按照用户分组
      //使用函数计算RFE的值
      .agg(
        max($"log_time").as("last_time"),
        //计算F值
        count($"loc_url").as("frequency"),
        //计算E值
        countDistinct($"loc_url").as("engagements")
      )
      .select(
        $"global_user_id".as("userId"),
        datediff(date_sub(current_timestamp(), 100), $"last_time").as("recency"),
        $"frequency",
        $"engagements"
      )
    /*
    root
    |-- userId: string (nullable = true)
    |-- recency: integer (nullable = true)
    |-- frequency: long (nullable = false)
    |-- engagements: long (nullable = false)
    */
    /*
    +------+-------+---------+-----------+
    |userId|recency|frequency|engagements|
    +------+-------+---------+-----------+
    |1 |23 |418 |270 |
    |102 |23 |415 |271 |
    |107 |23 |424 |280 |
    |110 |23 |356 |249 |
    |111 |23 |381 |257 |
    |120 |23 |386 |269 |
    */

    // 2. 按照规则，给RFE值打分: Score
    /*
    R:0-15天=5分，16-30天=4分，31-45天=3分，46-60天=2分，大于61天=1分
    F:≥400=5分，300-399=4分，200-299=3分，100-199=2分，≤99=1分
    E:≥250=5分，200-249=4分，150-199=3分，149-50=2分，≤49=1分
    */
    // R 打分条件表达式
    val rWhen: Column = when(col("recency").between(1, 15), 5.0).
      when(col("recency").between(16, 30), 4.0).
      when(col("recency").between(31, 45), 3.0).
      when(col("recency").between(46, 60), 2.0).
      when(col("recency").geq(61, 15), 1.0)
    // F 打分条件表达式
    val fWhen = when(col("frequency").leq(99), 1.0) //
      .when(col("frequency").between(100, 199), 2.0) //
      .when(col("frequency").between(200, 299), 3.0) //
   .when(col("frequency").between(300, 399), 4.0) //
      .when(col("frequency").geq(400), 5.0) //
    // M 打分条件表达式
    val eWhen = when(col("engagements").lt(49), 1.0) //
      .when(col("engagements").between(50, 149), 2.0) //
      .when(col("engagements").between(150, 199), 3.0) //
      .when(col("engagements").between(200, 249), 4.0) //
      .when(col("engagements").geq(250), 5.0) //

    val rfeScoreDF: DataFrame = rfeDF.select(
      $"userId",
      rWhen.as("r_score"),
      fWhen.as("f_score"),
      eWhen.as("e_score")
    )

    // 3 组合rfe列为特征值features
    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("r_score","f_score","e_score"))
      .setOutputCol("features")
    val rfeFeaturesDF: DataFrame = assembler.transform(rfeScoreDF)

    // 4 获取kMeans模型
    val kMeans = new KMeans()
    val kMeansModel: KMeansModel = kMeans.setK(4)
      .setFeaturesCol("features")
      .setPredictionCol("predict")
      .setMaxIter(20)
      .fit(rfeFeaturesDF)
    // 5 模型预测
    val predictions = kMeansModel.transform(rfeFeaturesDF)
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)

    println(s"Silhouette with squared euclidean distance = $silhouette")

    // 6 打标签
    //获取聚类模型中簇中心及索引
    val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
    val centerIndexArray: Array[((Int, Double), Int)] = clusterCenters
      .zipWithIndex
      .map{case(vector, centerIndex) => (centerIndex,
        vector.toArray.sum)}
      .sortBy{case(_, rfm) => - rfm}
      .zipWithIndex
    //centerIndexArray.foreach(println)
    // 聚类类簇关联属性标签数据rule，对应聚类类簇与标签tagId
    val indexTagMap: Map[Int, String] = centerIndexArray
      .map{case((centerIndex, _), index) =>
        val tagName = rulesMap(index.toString)
        (centerIndex, tagName)
      }
      .toMap
    val indexTagMapBroadcast: Broadcast[Map[Int, String]] = spark.sparkContext.broadcast(indexTagMap)
    // 关联聚类预测值，将prediction转换为tagId
    val index_to_tag: UserDefinedFunction = udf((prediction => indexTagMapBroadcast.value(prediction)))
    val modelDf: DataFrame = predictions.select(
      $"userId",
      index_to_tag($"prediction").as("tagId")
    )
    modelDf
  }
}

object RfeModel{
  def main(args: Array[String]): Unit = {
    val model = new RfeModel()
    model.executeModel(367L)
  }
}