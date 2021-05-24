package com.rison.tags.util

import java.util
import java.util.Map

import com.rison.tags.config.ModelConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters.asScalaSetConverter

/**
 * @author : Rison 2021/5/24 上午9:23
 *         创建SparkSession对象工具类
 */
object SparkUtils {
  /**
   * 加载Spark Application默认配置文件，设置到SparkConf中
   *
   * @param resource 资源配置文件名称
   * @return SparkConf对象
   */
  def loadConf(resource: String): SparkConf = {
    // 1 创建SparkConf对象
    val sparkConf = new SparkConf()

    // 2 使用configFactory加载配置文件
    val config: Config = ConfigFactory.load(resource)

    //3 获取加载配置信息
    val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()

    //4 循环遍历设置属性值到SparkConf中
    import scala.collection.JavaConversions._
    entrySet.asScala.foreach { entry =>
      // 获取属性来源的文件名称
      val resourceName = entry.getValue.origin().resource()
      if (resource.equals(resourceName)) {
        sparkConf.set(entry.getKey,
          entry.getValue.unwrapped().toString)
      }
    }
    // 5. 返回SparkConf对象
    sparkConf
  }

  /**
   * 构建SparkSession实例对象，如果是本地模式，设置master
   *
   * @return
   */
  def createSparkSession(clazz: Class[_], isHive: Boolean = false):
  SparkSession = {
    // 1. 构建SparkConf对象
    val sparkConf: SparkConf = loadConf(resource = "spark.properties")
    // 2. 判断应用是否是本地模式运行，如果是设置
    if (ModelConfig.APP_IS_LOCAL) {
      sparkConf.setMaster(ModelConfig.APP_SPARK_MASTER)
    }
    // 3. 创建SparkSession.Builder对象
    var builder: SparkSession.Builder = SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .config(sparkConf)
    // 4. 判断应用是否集成Hive，如果集成，设置Hive MetaStore地址
    // 如果在config.properties中设置集成Hive，表示所有SparkApplication都集成Hive；否则判断isHive，表示针对某个具体应用是否集成Hive
    if (ModelConfig.APP_IS_HIVE || isHive) {
      builder = builder
        .config("hive.metastore.uris",
          ModelConfig.APP_HIVE_META_STORE_URL)
        .enableHiveSupport()
    }
    // 5. 获取SparkSession对象
    val session = builder.getOrCreate()
    // 6. 返回
    session
  }
}
