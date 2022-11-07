package com.spark_scala

import org.apache.spark.sql.functions._

object EqualExpert {
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.{DataFrame, SparkSession}

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("Spark Session is being Created")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("EqualExpert")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println("Spark Session Created Successfully")
    println("Reading json file from local device")

    val votes_df = spark.read.option("multiline", "true").json("C://Users/Consultant/Downloads/Votes.json")
    println("Votes Schema:")
    votes_df.printSchema()
    println("Votes Table")
    println("----------------")
    votes_df.show()

    val sfOptions = Map(
      "sfURL" -> "https://rmb31792.us-east-1.snowflakecomputing.com",
      "sfUser" -> "HusseinGhosn",
      "sfPassword" -> "Hussein1234",
      "sfDatabase" -> "EQUALEXPERTS_DB",
      "sfSchema" -> "EQUALEXPERTS_TABLE",
      "sfWarehouse" -> "HUSSEIN",
      "sfRole" -> "accountadmin"
    )
    println("Writing VOTES_TABLE into snowflake......")
    votes_df.write
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("dbtable", "VOTES_TABLE")
      .mode("overwrite")
      .save()
    println("VOTES_TABLE writing complete")


    println("Read the required columns for the snowflake table VOTES_TABLE")
    val df: DataFrame = spark.read
          .format("net.snowflake.spark.snowflake") // or just use "snowflake"
          .options(sfOptions)
          .option("query", "select PostId, VoteTypeId, CreationDate from VOTES_TABLE")
          .load()
//  Convert Id columns to integer value and split the date into year-month-day format
    val df1 = df.select(col("PostId").cast("int"),
      col("VoteTypeId").cast("int"),
      to_date(split(col("CreationDate"), "T").getItem(0)).as("Date"))
    println("SPARK_VOTES_SCHEMA")
    df1.printSchema()
    println("SPARK_VOTES_TABLE")
    df1.show()

    println("Preprocessing of Votes_table started")
    val data = df1.select(col("PostId"),
      col("VoteTypeId"),
      weekofyear(col("Date")).as("Week")).groupBy(col("VoteTypeId"), col("week")).agg(count(col("VoteTypeId")))
//      .groupBy(col("PostId"), col("Week"))
//      .agg(mean("VoteTypeId").alias("MeanVotes"))
//      .orderBy(col("PostId"), col("Week"))
    println("Preprocessing Schema")
    data.printSchema()
    println("Preprocessing Table")
    println("--------------------")
    data.show()

//    println("Write preprocessed data into snowflake")
//    data.write
//      .format("net.snowflake.spark.snowflake")
//      .options(sfOptions)
//      .option("dbtable", "VOTES_TABLE_Processed")
//      .mode("overwrite")
//      .save()
//    println("Writing of preprocessed complete")
    data
  }
}
