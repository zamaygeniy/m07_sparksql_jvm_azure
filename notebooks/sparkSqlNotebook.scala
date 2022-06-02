// Databricks notebook source
// MAGIC %md
// MAGIC Configuration for dbutils

// COMMAND ----------

//Congifuration for access to Azure Data Lake
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "8c280f98-faac-441a-aa69-b00a42351332",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope="sparksql",key="storagekey"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/5dc174ab-3668-4603-a7f3-fa63ad86d976/oauth2/token")

//Mount storage object
dbutils.fs.mount(
  source = "abfss://data@stavalswesteurope.dfs.core.windows.net/",
  mountPoint = "/mnt/data",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %md
// MAGIC Load and write expedia data to Delta table

// COMMAND ----------

val read_format = "com.databricks.spark.avro"
val write_format = "delta"
val load_path = "/mnt/data/expedia/*"
val save_path = "/tmp/delta/expedia"
val table_name = "default.expedia"

// Load the data from its source.
val expedia = spark
  .read
  .format(read_format)
  .load(load_path)

// Write the data to its target.
expedia.write
  .format(write_format)
  .mode("overwrite")
  .save(save_path)

// Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " USING DELTA LOCATION '" + save_path + "'")

// COMMAND ----------

// MAGIC %md
// MAGIC Filter expedia data and create table for it

// COMMAND ----------

import io.delta.implicits._
import org.apache.spark.sql.functions._

val write_format = "delta"
val save_path = "/tmp/delta/filteredExpedia"
val table_name = "default.filteredExpedia"

//Regular expression for checkin and checkout date
val regex = "([0-9]{4}-[0-9]{2}-[0-9]{2})"

//Filter expedia data by valid checkin and checkout date
//Calculate differnce between checkin and checkout date and filter by diff >= 7
val filteredExpedia = expedia.filter(col("srch_ci").rlike(regex) && col("srch_co").rlike(regex))
                             .withColumn("dateDiff", datediff(col("srch_co"), col("srch_ci")))
                             .filter(col("dateDiff") >= 7)

filteredExpedia.write
               .format(write_format)
               .mode("overwrite")
               .save(save_path)

spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " USING DELTA LOCATION '" + save_path + "'")


// COMMAND ----------

// MAGIC %md
// MAGIC Load and write hotel-weather to Delta table

// COMMAND ----------

val read_format = "parquet"
val write_format = "delta"
val load_path = "/mnt/test/hotel-weather/"
val save_path = "/tmp/delta/hotel-weather"
val table_name = "default.hotelWeather"

// Load the data from its source.
val hotelWeather = spark.read
                        .format(read_format)
                        .load(load_path)

// Write the data to its target.
hotelWeather.write
            .format(write_format)
            .mode("overwrite")
            .save(save_path)

// Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " USING DELTA LOCATION '" + save_path + "'")

// COMMAND ----------

// MAGIC %md
// MAGIC **For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.**

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM(
// MAGIC      SELECT address,
// MAGIC             city,
// MAGIC             country,
// MAGIC             geoHash,
// MAGIC             hotels.id AS hotels_id,
// MAGIC             latitude,
// MAGIC             longitude,
// MAGIC             name,
// MAGIC             book.id AS book_id,
// MAGIC             srch_ci,
// MAGIC             srch_co,
// MAGIC             dateDiff,
// MAGIC             AVG(avg_tmpr_c) OVER (PARTITION BY hotels.id, srch_ci, srch_co, book.id  ORDER BY hotels.id ASC) AS avg_tmpr_during_stay,
// MAGIC             FIRST_VALUE(avg_tmpr_c) OVER (PARTITION BY hotels.id, srch_ci, srch_co, book.id ORDER BY hotels.id ASC) -
// MAGIC             LAST_VALUE(avg_tmpr_c) OVER (PARTITION BY hotels.id, srch_ci, srch_co, book.id ORDER BY hotels.id ASC) AS weather_trend
// MAGIC      FROM default.`hotelWeather` AS hotels
// MAGIC      JOIN default.`filteredExpedia` as book
// MAGIC      ON book.hotel_id = hotels.id
// MAGIC      AND wthr_date >= srch_ci
// MAGIC      AND wthr_date <= srch_co
// MAGIC ) 
// MAGIC GROUP BY address,
// MAGIC          city,
// MAGIC          country,
// MAGIC          geoHash,
// MAGIC          hotels_id,
// MAGIC          latitude,
// MAGIC          longitude,
// MAGIC          name,
// MAGIC          book_id,
// MAGIC          srch_ci,
// MAGIC          srch_co,
// MAGIC          dateDiff,
// MAGIC          avg_tmpr_during_stay,
// MAGIC          weather_trend

// COMMAND ----------

// MAGIC %md
// MAGIC Filer and flatMap expedia data to get booking for each month. Create Delta table for new data.

// COMMAND ----------

import io.delta.implicits._
import scala.collection.mutable.ListBuffer 
import java.time.LocalDate
import org.apache.spark.sql.functions._

//Regular expression for checkin and checkout date
val regex = "([0-9]{4}-[0-9]{2}-[0-9]{2})"

//Filter expedia data frame by valid checkin and checkout date 
//FlatMap valid data to get range of months reserved by one record
val visitMonths = expedia.filter(col("srch_ci").rlike(regex) && col("srch_co").rlike(regex))
                         .flatMap(f => {
                           
                           //Get hotelId, checkin and checkout date from row
                           val hotelId = f.getLong(19)
                           val checkinDate = LocalDate.parse(f.getString(12)).withDayOfMonth(1)
                           val checkoutDate = LocalDate.parse(f.getString(13)).withDayOfMonth(1)
                           var date = checkinDate
                           var resultBufferList = new ListBuffer[(Long, Int, Int)]
                           
                           //Loop to get every month from checkin to checkout date
                           while(date.isBefore(checkoutDate)){
                             resultBufferList += ((hotelId, date.getYear(), date.getMonth().getValue()))
                             date = date.plusMonths(1)
                           }
                           resultBufferList.toList
                         })
                         .withColumnRenamed("_1", "hotelId")
                         .withColumnRenamed("_2", "year")
                         .withColumnRenamed("_3", "month")

val write_format = "delta"
val save_path = "/tmp/delta/visitMonths"
val table_name = "default.visitMonths"

// Write the data to its target.
visitMonths.write
           .format(write_format)
           .mode("overwrite")
           .save(save_path)

// Create the table.
spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " USING DELTA LOCATION '" + save_path + "'")


// COMMAND ----------

// MAGIC %md
// MAGIC **Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.**

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT address, 
// MAGIC        city, 
// MAGIC        country, 
// MAGIC        geoHash, 
// MAGIC        id, 
// MAGIC        longitude, 
// MAGIC        latitude,
// MAGIC        name,
// MAGIC        ranking.year,
// MAGIC        ranking.month,
// MAGIC        ranking.booking_counter,
// MAGIC        ranking.rank
// MAGIC FROM default.`hotelweather`
// MAGIC JOIN
// MAGIC     (SELECT *,
// MAGIC             row_number() OVER (PARTITION BY month, year ORDER BY booking_counter DESC) AS rank
// MAGIC      FROM
// MAGIC          (SELECT *, 
// MAGIC                  count(*) AS booking_counter
// MAGIC           FROM default.`visitMonths`
// MAGIC           GROUP BY hotelId, year, month
// MAGIC          )
// MAGIC     ) AS ranking
// MAGIC ON id = ranking.hotelId
// MAGIC WHERE rank <= 10
// MAGIC GROUP BY address, 
// MAGIC          city, 
// MAGIC          country, 
// MAGIC          geoHash, 
// MAGIC          id, 
// MAGIC          longitude, 
// MAGIC          latitude,
// MAGIC          name,
// MAGIC          ranking.year,
// MAGIC          ranking.month,
// MAGIC          ranking.booking_counter,
// MAGIC          ranking.rank

// COMMAND ----------

// MAGIC %md
// MAGIC **Top 10 hotels with max absolute temperature difference by month.**

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM(SELECT *,
// MAGIC             row_number() OVER (PARTITION BY month, year ORDER BY abs_diff_tmpr_c DESC) AS rank 
// MAGIC      FROM (SELECT 
// MAGIC                   address, 
// MAGIC                   city, 
// MAGIC                   country, 
// MAGIC                   geoHash, 
// MAGIC                   id, 
// MAGIC                   longitude, 
// MAGIC                   latitude,
// MAGIC                   name,
// MAGIC                   month, 
// MAGIC                   year,
// MAGIC                   MAX(avg_tmpr_c) OVER (PARTITION BY name, month, year ORDER BY year ASC, month ASC) -
// MAGIC                   MIN(avg_tmpr_c) OVER (PARTITION BY name, month, year ORDER BY year ASC, month ASC) AS abs_diff_tmpr_c
// MAGIC            FROM default.`hotelWeather`)
// MAGIC      GROUP BY address, city, country, geoHash, id, longitude, latitude, name, month, year, abs_diff_tmpr_c
// MAGIC ) 
// MAGIC WHERE rank <= 10 

// COMMAND ----------

//Unmount storage object
dbutils.fs.unmount("/mnt/data")
