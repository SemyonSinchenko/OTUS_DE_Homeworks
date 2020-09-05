package otus.sinchenko.opp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Reader extends (() => RDD[SAPRow]) {
  private lazy val fileName = "all_stocks_5yr.csv"
  private lazy val spark = SparkSession.builder().getOrCreate()

  override def apply(): RDD[SAPRow] = {
    spark.sparkContext.textFile(fileName)
      .filter(!_.startsWith("date"))
      .map(
        (r: String) => r.split(",")
      )
      .filter(!_.map(_.isEmpty).reduce(_ || _))
      .map(
        (r: Array[String]) => SAPRow(
          r(0),
          r(1).toDouble,
          r(2).toDouble,
          r(3).toDouble,
          r(4).toDouble,
          r(5).toDouble,
          r(6)
        )
    )
  }
}