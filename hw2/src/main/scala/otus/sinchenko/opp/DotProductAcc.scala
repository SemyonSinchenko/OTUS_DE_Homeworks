package otus.sinchenko.opp

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.util.AccumulatorV2
import collection.JavaConversions._

class DotProductAcc()
  extends AccumulatorV2[Iterable[SAPRow], ConcurrentHashMap[(String, String), Double]]
    with Serializable
{
  val acc = new ConcurrentHashMap[(String, String), Double]()

  override def isZero: Boolean = acc.isEmpty

  override def copy(): AccumulatorV2[Iterable[SAPRow], ConcurrentHashMap[(String, String), Double]] = {
    val other = new DotProductAcc
    for (v <- acc.entrySet()) {
      other.acc.put(v.getKey, v.getValue)
    }
    other
  }

  override def add(v: Iterable[SAPRow]): Unit = {
    v.foreach(r1 => {
      v.filter(_.name <= r1.name).foreach(
        r2 => {
          val mid1 = (r1.open + r1.close) / 2
          val mid2 = (r2.open + r2.close) / 2

          acc.put(
            (r1.name, r2.name), acc.getOrDefault((r1.name, r2.name), .0) + mid1 * mid2
          )
        }
      )
    })
  }

  override def value: ConcurrentHashMap[(String, String), Double] = acc

  override def merge(other: AccumulatorV2[Iterable[SAPRow], ConcurrentHashMap[(String, String), Double]]): Unit = {
    for (v <- other.value.entrySet()) {
      acc.put(v.getKey, acc.getOrDefault(v.getKey, .0) + v.getValue)
    }
  }

  override def reset(): Unit = acc.clear()
}
