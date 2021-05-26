package com.rison.tags.models.ml

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer, VectorIndexerModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author : Rison 2021/5/26 下午3:31
 *官方案例 决策数据分类算法
 */
object DecisionTreeTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dataFrame: DataFrame = spark.read
      .format("libsvm")
      .load("datas/mllib/sample_libsvm_data.txt")
    dataFrame.printSchema()
    dataFrame.show(10, truncate = false)

    // 2 特征工程 特征提取， 特征转换及特征选择
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("index_label")
      .fit(dataFrame)
    val df1: DataFrame = labelIndexer.transform(dataFrame)
    df1.printSchema()
    df1.show(10, truncate = false)

    //对类别特征数据进行特殊处理，当每列的值的个数小于设置k,那么此列数据被当做类别特征，自动进行索引转换
    val featureIndexer: VectorIndexerModel = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("index_features")
      .setMaxCategories(4)
      .fit(df1)

    val df2: DataFrame = featureIndexer.transform(df1)

    df2.printSchema()
    df2.show(10, truncate = false)

    //3 划分数据集 训练数据和测试数据
    val Array(trainDF, testingDF): Array[Dataset[Row]] = df2.randomSplit(Array(0.8, 0.2))

    //4 使用决策树算法构建分类模型
    val dtc = new DecisionTreeClassifier()
      .setLabelCol("index_label")
      .setFeaturesCol("index_features")
    //设置决策树算法相关超参
      .setMaxDepth(5)
      .setMaxBins(32)  //此值必须大于等于类别特征个数
      .setImpurity("gini") //也可以是香农熵：entropy

    val dtcModel: DecisionTreeClassificationModel = dtc.fit(trainDF)
    println(dtcModel.toDebugString)

    // 5 模型评估，计算准确度
    val predictionDF: DataFrame = dtcModel.transform(testingDF)
    predictionDF.printSchema()
    predictionDF.select(
      $"label",$"index_label",$"probability",$"prediction"
    ).show(20, truncate = false)

    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("index_label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy: Double = evaluator.evaluate(predictionDF)

    println(s"Accuracy = $accuracy")




    spark.stop()

  }
}
