package io.delta.sharing.spark

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class LakeDeltaSharingSuite extends QueryTest with SharedSparkSession with DeltaSharingIntegrationTest {

  integrationTest("csv_tbl") {
    val tablePath = testProfileFile.getCanonicalPath + "#hackathon_share.hackathon_db.csv_tbl"

    var df = spark.read.format("deltaSharing").option("header", true).load(tablePath)
    df.printSchema()
    df.show()
  }

  integrationTest("orc_tbl") {
    val tablePath = testProfileFile.getCanonicalPath + "#hackathon_share.hackathon_db.orc_tbl"

    var df = spark.read.format("deltaSharing").load(tablePath)
    df.printSchema()
    df.show()
  }

  integrationTest("json_tbl") {
    val tablePath = testProfileFile.getCanonicalPath + "#hackathon_share.hackathon_db.json_tbl"

    var df = spark.read.format("deltaSharing").load(tablePath)
    df.printSchema()
    df.show()
  }

  integrationTest("parquet_tbl") {
    val tablePath = testProfileFile.getCanonicalPath + "#hackathon_share.hackathon_db.parquet_tbl"

    var df = spark.read.format("deltaSharing").load(tablePath)
    df.printSchema()
    df.show()
  }

  integrationTest("avro_tbl") {
    val tablePath = testProfileFile.getCanonicalPath + "#hackathon_share.hackathon_db.avro_tbl"

    var df = spark.read.format("deltaSharing").load(tablePath)
    df.printSchema()
    df.show()
  }

}
