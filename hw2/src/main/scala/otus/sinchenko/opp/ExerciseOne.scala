package otus.sinchenko.opp

import org.apache.spark.rdd.RDD

object ExerciseOne extends (RDD[SAPRow] => Unit) {
  override def apply(v1: RDD[SAPRow]): Unit = {
    v1.map(r => r.name -> r)
      .aggregateByKey((.0, .0))(
        (acc: (Double, Double), r: SAPRow) => {
          (
            if (acc._1 >= r.high) acc._1 else r.high,
            if (acc._2 <= r.low) acc._2 else r.low
          )
        },
        (acc1: (Double, Double), acc2: (Double, Double)) => {
          (
            if (acc1._1 >= acc2._1) acc1._1 else acc2._1,
            if (acc1._2 <= acc2._2) acc1._2 else acc2._2
          )
        }
      )
      .map(f => f._1 -> (f._2._1 - f._2._2))
      .sortBy(_._2, ascending = false)
      .zipWithIndex()
      .filter(f => f._2 <= 3)
      .map(_._1._1)
      .saveAsTextFile("2a.txt")

      v1.groupBy(r => r.name).map{case (k: String, vs: Iterable[SAPRow]) => {
        k -> {
          val sortedSeq = vs.toArray.sortBy(_.date).reverse

          sortedSeq.zipWithIndex.tail.map{case (r: SAPRow, idx: Int) => {
            val prev = sortedSeq(idx - 1).close
            if (prev.compareTo(.0) != 0) {
              r.close / prev - 1
            } else {
              .0
            }
          }}.max
        }
      }}.sortBy(_._2, ascending = false)
        .zipWithIndex()
        .filter(f => f._2 <= 3)
        .map(_._1._1)
        .saveAsTextFile("2b.txt")
  }
}
