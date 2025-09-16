package org.dahua.iceberg.spark.local

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.dahua.iceberg.spark.local.Util.{db, sparkSession, tb}


object BeforeRewriteDataFiles {
  def main(args: Array[String]): Unit = {
    Util.createPartitionTable()
    Util.insertTable(1, 10, 1)
    Util.insertTable(11, 20, 1)
    Util.insertTable(1, 10, 1)
    Util.insertTable(11, 20, 1)

    sparkSession.sql(s"delete from $db.$tb where id = 1")
  }
}
