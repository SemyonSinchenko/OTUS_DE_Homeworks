package otus.sinchenko.opp

import org.apache.spark.sql.SparkSession

object SparkSessionInitializer extends (() => Unit) {
  override def apply(): Unit = {
    SparkSession
      .builder()
      .appName("SinchenkoHW2App")
      .getOrCreate()
  }
}
