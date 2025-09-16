package org.dahua.iceberg.spark.local

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.dahua.iceberg.spark.local.Util.{db, sparkSession, tb}


object RewriteDataFiles {
  def main(args: Array[String]): Unit = {
//    Util.createPartitionTable()
//    Util.insertTable(1, 10, 1)
//    Util.insertTable(11, 20, 1)

    sparkSession.sql("CALL spark_catalog.system.rewrite_data_files(" +
      s"table => '$db.$tb', " +
      "strategy => 'sort', " +
      "where => 'dt = 1',  " +
      "sort_order => 'id DESC NULLS LAST', "  +
      "options => map('min-input-files', '2', 'target-file-size-bytes', '536870912') " +
      ")")
  }
}
