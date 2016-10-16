import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by andrii_korkoshko on 16.10.16.
  */
class SparkService {

  val sc: SparkContext = createSparContext();


  def getLineNumber(): Long = {
    val rdd: RDD[String] = getTripsRdd
    return rdd.count();
  }

  def getTripsRdd: RDD[String] = {
    val rdd: RDD[String] = sc.textFile("data/taxi/trips.txt")
    rdd
  }

  def getBostonTripsCount(): Long = {
    val tripsRdd: RDD[String] = getTripsRdd
    return getBostonTripsMoreThen10(tripsRdd).count();
  }

  def getBostonTripsMoreThen10(tripsRdd: RDD[String]): RDD[String] = {
    getBostonTrips(tripsRdd).filter(_.split(" ").apply(2).toInt > 10)
  }

  def getBostonTrips(tripsRdd: RDD[String]): RDD[String] = {
    tripsRdd.filter(_.split(" ").apply(1).equalsIgnoreCase("Boston"))
  }

  def getBostonTripsLength(): Double = {
    val trips: RDD[String] = getBostonTrips(getTripsRdd)
    return trips.map(_.split(" ").apply(2).toInt).sum();
  }

  def getMostDrivers(): Array[(Int,String)] = {
    val tripsRdd: RDD[String] = getTripsRdd
    val driverTrips: RDD[(String, Int)] = tripsRdd.map(_.split(" ")).map(arr => (arr(0), arr(2).toInt)).reduceByKey(_ + _)
    val driverFileRDD: RDD[String] = sc.textFile("data/taxi/drivers.txt")
    val drivers: RDD[(String, String)] = driverFileRDD.map(_.split(",")).map(arr=>(arr(0),arr(1)))
    return  driverTrips.join(drivers).map(_._2).sortByKey().take(3);
  }

  def createSparContext(): SparkContext = {
    val conf: SparkConf = new SparkConf();
    conf.setAppName("Andrii_K")
    conf.setMaster("local")
    val sc: SparkContext = new SparkContext(conf);

    return sc;
  }

}
