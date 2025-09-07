package org.dahua.iceberg.spark.local

import org.apache.spark.sql.SparkSession

object Util {
  val db = "i_db"
  val tb = "test"

  val sparkSession: SparkSession = createSparkSession

  def createSparkSession(): SparkSession = {
    SparkSession
      .builder
      .master("local[2]")
      .appName("spark-iceberg test")
      .config("spark.master.ui.port", "4040")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hadoop")
      .config("spark.sql.warehouse.dir", "file:/D:/projects/iceberg/iceberg_spark/spark-warehouse")
      .config("spark.sql.catalog.spark_catalog.warehouse", "file:/D:/projects/iceberg/iceberg_spark/spark-warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.sql.codegen.wholeStage", "false")
      .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
      .config("spark.default.parallelism", "1")
      .config("spark.sql.shuffle.partitions", "1").getOrCreate
  }

  def createPartitionTable(): Unit = {
    sparkSession.sql(s"create database if not exists $db")
    sparkSession.sql(s"drop table if exists $db.$tb purge")
    sparkSession.sql(s"create table $db.$tb(id int, name string) using iceberg partitioned by(dt string)" +
      s"tblproperties('write.merge.mode' = 'merge-on-read')")
  }

  def insertTable(): Unit = {
    sparkSession.sql(s"insert into $db.$tb values(1, 'zs', 'dt'), (2, 'lisi', 'dt')")
  }

  def insertTable(start: Int, end: Int, step: Int): Unit = {
    val sql = StringBuilder.newBuilder
    sql.append(s"insert into $db.$tb values")
    for (i <- start to end by step) {
      sql.append(s"($i, 'name_$i', 'dt_0')")
      if (i < end) {
        sql.append(",")
      }
    }

    sparkSession.sql(sql.toString())
  }
}
