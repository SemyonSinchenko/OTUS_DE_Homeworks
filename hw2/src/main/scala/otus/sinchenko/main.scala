package otus.sinchenko

import opp.{ExerciseOne, Reader, SparkSessionInitializer}

object Main extends App {
  SparkSessionInitializer()
  ExerciseOne(Reader())
}