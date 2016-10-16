/**
  * Created by andrii_korkoshko on 16.10.16.
  */
object Main {

  def main(args: Array[String]): Unit = {
    val sparkService: SparkService = new SparkService
    println("Hi")
    println(s"Line count ${sparkService.getLineNumber()}")
    println(s"Boston trips count ${sparkService.getBostonTripsCount()}")
    println(s"Boston trips length ${sparkService.getBostonTripsLength()}")
    println(s"Most drivers ${sparkService.getMostDrivers()}")
  }

}
