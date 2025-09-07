package org.dahua.iceberg.spark.local

import org.dahua.iceberg.spark.local.Util.{db, sparkSession, tb}

object SparkIceberg {

  def main(args: Array[String]): Unit = {
    Util.createPartitionTable()
    Util.insertTable(1, 10, 1)
    Util.insertTable(11, 20, 1)

    sparkSession.sql(s"select * from $db.$tb").show(100, false)
  }
}
