package org.dahua.iceberg.spark.local

import org.apache.spark.sql.SparkSession
import org.dahua.iceberg.spark.local.Util.{db, sparkSession, tb}

object SparkIceberg {

  def main(args: Array[String]): Unit = {
    Util.createPartitionTable()
    Util.insertTable()

    sparkSession.sql(s"select * from $db.$tb").show(100, false)
  }
}
