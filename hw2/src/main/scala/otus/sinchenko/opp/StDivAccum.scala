package otus.sinchenko.opp

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.util.AccumulatorV2

import scala.collection.JavaConversions._

class StDivAccum extends AccumulatorV2[Iterable[SAPRow], ConcurrentHashMap[String, Double]]
  with Serializable
{
  val acc = new ConcurrentHashMap[String, Double]()

  override def isZero: Boolean = acc.isEmpty

  override def add(v: Iterable[SAPRow]): Unit = {
    v.foreach(
      vv => {
        acc.put(vv.name, acc.getOrDefault(vv.name, .0) + {
          val single = (vv.open + vv.close) / 2
          single * single
        })
      }
    )
  }

  override def value: ConcurrentHashMap[String, Double] = acc

  override def merge(other: AccumulatorV2[Iterable[SAPRow], ConcurrentHashMap[String, Double]]): Unit = {
    for (v <- other.value.entrySet()) {
      acc.put(v.getKey, {
        acc.getOrDefault(v.getKey, .0) + v.getValue
      })
    }
  }

  override def copy(): AccumulatorV2[Iterable[SAPRow], ConcurrentHashMap[String, Double]] = {
    val other = new StDivAccum
    for (v <- acc.entrySet()) {
      other.acc.put(v.getKey, v.getValue)
    }

    other
  }

  override def reset(): Unit = {
    acc.clear()
  }
}
