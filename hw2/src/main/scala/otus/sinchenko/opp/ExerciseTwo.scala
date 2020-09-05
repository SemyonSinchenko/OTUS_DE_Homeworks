package otus.sinchenko.opp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ExerciseTwo extends (RDD[SAPRow] => Unit) {
  override def apply(v1: RDD[SAPRow]): Unit = {
    val names = v1.map(_.name).distinct().collect()

    SparkSession.builder().getOrCreate().sparkContext.parallelize(
      names.flatMap(n1 => {
        names.filter(n => n <= n1).map(
          n2 => (n1, n2) -> {
            val rdd1 = v1.filter(f => f.name == n1).map(f => f.date -> {
              (f.open + f.close) / 2
            })

            val rdd2 = v1.filter(f => f.name == n2).map(f => f.date -> {
              (f.open + f.close) / 2
            })

            correlation(rdd1, rdd2)
          }
        )
      }).sortBy(_._2).reverse.take(3).map(_._1)
    ).saveAsTextFile("3.txt")
  }

  private def correlation(v1: RDD[(String, Double)], v2: RDD[(String, Double)]): Double = {
    val joined = v1.join(v2)
    val norms = joined.map(_._2).reduce{case ((acc1: Double, acc2: Double), (val1: Double, val2: Double)) => {
      (acc1 + val1 * val1, acc2 + val2 * val2)
    }}

    joined
      .map(_._2)
      .map(f => f._1 * f._2)
      .reduce{case (acc: Double, v: Double) => acc + v} / Math.sqrt(norms._1 * norms._2)
  }
}
