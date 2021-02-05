package com.intel.oap.tpch

import com.intel.oap.tpch.util.{TPCHRunner, TPCHTableGen}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object TPCHSparkAPP {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.memory.offHeap.size", "6G")
        .set("spark.sql.extensions", "com.intel.oap.ColumnarPlugin")
        .set("spark.sql.codegen.wholeStage", "false")
        .set("spark.sql.sources.useV1SourceList", "")
        .set("spark.sql.columnar.tmp_dir", "/tmp/")
        .set("spark.sql.adaptive.enabled", "false")
        .set("spark.sql.columnar.sort.broadcastJoin", "true")
        .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
        .set("spark.executor.heartbeatInterval", "3600000")
        .set("spark.network.timeout", "3601s")
        .set("spark.oap.sql.columnar.preferColumnar", "true")
        .set("spark.sql.columnar.codegen.hashAggregate", "false")
        .set("spark.sql.columnar.sort", "true")
        .set("spark.sql.columnar.window", "true")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .set("spark.unsafe.exceptionOnMemoryLeak", "false")
        .set("spark.network.io.preferDirectBufs", "false")
    val spark = SparkSession
        .builder()
        .config(conf)
        .appName("MyAPP")
        .getOrCreate()

    val gen = new TPCHTableGen(spark, 0.1, "/mnt/DP_disk2/tpch_generated")
    gen.gen()
    val runner = new TPCHRunner(spark)

    runner.runTPCHQuery(1, 1, false)
  }
}
