package otus.sinchenko

import opp.{ExerciseOne, ExerciseTwo, Reader, SparkSessionInitializer}

object Main extends App {
  SparkSessionInitializer()
  val rdd = Reader()
  ExerciseOne(rdd)
  ExerciseTwo(rdd)
}