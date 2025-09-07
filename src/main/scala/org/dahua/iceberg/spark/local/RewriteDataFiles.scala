package org.dahua.iceberg.spark.local

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.dahua.iceberg.spark.local.Util.{db, sparkSession, tb}


object RewriteDataFiles {



  def main(args: Array[String]): Unit = {
    val table = Spark3Util.loadIcebergTable(sparkSession, s"$db.$tb")
    SparkActions
      .get()
      .rewriteDataFiles(table)
      //      .filter(Expressions.equal("dt", "test.dt_0"))
      .option("target-file-size-bytes", String.valueOf(512 * 1024 * 1024)) // 目标输出大小 512MB
      .option("min-input-files", "2")   // 文件组中重新的最少文件数量
      .option("min-file-size-bytes", "1")  // 大小超过此阈值的文件都将被考虑重写。默认75% of target file size
      .option("max-file-size-bytes", "636870912")  // 大小超过此阈值的文件都将被考虑重写。默认180% of target file size
      .option("max-concurrent-file-group-rewrites", "5")
      .option("partial-progress.enabled", "false")
      .execute();
  }
}
