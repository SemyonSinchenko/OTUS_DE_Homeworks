package otus.sinchenko.opp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import collection.JavaConversions._
import scala.collection.mutable

object ExerciseTwo extends (RDD[SAPRow] => Unit) {
  override def apply(v1: RDD[SAPRow]): Unit = {
    val accStd = new StDivAccum
    val accDot = new DotProductAcc

    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.register(accStd)
    spark.sparkContext.register(accDot)

    v1.groupBy(_.date).map(_._2).foreach(f => {
      accDot.add(f)
      accStd.add(f)
    })

    val stdDivs = accStd.value
    val dotProducts = accDot.value

    val correlations = new mutable.HashMap[(String, String), Double]()
    for (v <- dotProducts.entrySet()) {
      correlations(v.getKey) = {
        val den = stdDivs.get(v.getKey._1) * stdDivs.get(v.getKey._2)

        if (den.compareTo(.0) == 0) {
          .0
        } else {
          v.getValue / Math.sqrt(den)
        }
      }
    }

    spark.sparkContext.parallelize(
      correlations.toArray
        .sortBy(_._2).reverse
        .map(_._1)
        .take(3)
    ).saveAsTextFile("3.txt")
 }
}
