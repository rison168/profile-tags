package com.rison.tags.test.ml


import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{Normalizer, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @author : Rison 2021/5/23 下午2:40
 *鸢尾花数据集特征工程
 */
object IrisClassification {
  def main(args: Array[String]): Unit = {
    //构建SparkSession实例对象，通过构建者模式创建
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //For implicit conversions like converting RDD to DataFrames
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 自定义Schema信息
    val irisSchema: StructType = StructType(
      Array(
        StructField("sepal_length", DoubleType, nullable = true),
        StructField("sepal_width", DoubleType, nullable = true),
        StructField("petal_length", DoubleType, nullable = true),
        StructField("petal_width", DoubleType, nullable = true),
        StructField("category", StringType, nullable = true)
      )
    )
    // 1 加载数据集 文件属于csv格式，直接加载
    val rawIrisDF: DataFrame = spark.read
      .schema(irisSchema)
      .option("sep", ",")
      .option("encoding", "UTF-8")
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("datas/iris/iris.data")
//
//    rawIrisDF.printSchema()
    rawIrisDF.show(15, truncate = false)

    // 2 特征工程
    //类别特征转换StringIndexer
    val indexerModel = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("label")
      .fit(rawIrisDF)


    val df1: DataFrame = indexerModel.transform(rawIrisDF)
    df1.show(10)

    // 组合特征值 : VectorAssembler
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(rawIrisDF.columns.dropRight(1))
      .setOutputCol("raw_features")

    val rawFeaturesDF: DataFrame = assembler.transform(df1)

    rawFeaturesDF.printSchema()
    rawFeaturesDF.show(10, truncate = false)

    //特征正则话，使用L2正则
    val normalizer: Normalizer = new Normalizer()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setP(2.0)

    val featuresDF: DataFrame = normalizer.transform(rawFeaturesDF)

    featuresDF.printSchema()
    featuresDF.show(10, truncate = false)

    //将数据集缓存，LR算法迭代，使用多次

    featuresDF.persist(StorageLevel.MEMORY_AND_DISK).count()

    //3 使用逻辑回归算法训练模型
    val lr = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setMaxIter(15)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    //fit the model
    val lrModel: LogisticRegressionModel = lr.fit(featuresDF)

    //4 使用模型预测
    val predictionDF: DataFrame = lrModel.transform(featuresDF)

    predictionDF.select("label", "prediction")
      .show(150)

    // 5 模型评估 ： 准确度 = 预测正确的样本数/所有样本数
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    println(s"ACCU = ${evaluator.evaluate(predictionDF)}")

    // 模型保存与加载
    val modelPath = s"datas/models/lrModel-${System.currentTimeMillis()}"
    lrModel.save(modelPath)
    val model: LogisticRegressionModel = LogisticRegressionModel.load(modelPath)
    model.transform(
      Seq(
        Vectors.dense(Array(5.1,3.5,1.4,0.2))
      )
        .map(x => Tuple1.apply(x))
        .toDF("features")
    ).show(10, truncate = false)

    spark.stop()

  }
}
