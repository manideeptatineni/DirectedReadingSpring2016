import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import sparksparql.SparqlContext

/**
  * Created by machine1 on 4/28/16.
  */
//class for querying on each index and retrieving results
class QueryProcessing extends Serializable {


  def resultsExtraction(inputPath: String, temps: RDD[String], query: String, broadcast: Broadcast[SQLContext]) = {

    var queryProcessingResults = Set[Row]()

    def executeQuery1(index: Int, iterator: Iterator[(String)]): Iterator[Row] = {

      val dbpediaEndpoint = "mem:dataset=src/main/resources/indexes/index" + index + ".ttl"

      println("dbpediaEndpoint:" + dbpediaEndpoint)

      val sparqlContext = broadcast.value



      val dataFrame = sparqlContext.sparqlQuery(dbpediaEndpoint, query)

      val test = dataFrame.rdd.map(line => line.toString())

      //println(test.toString)
      dataFrame.map(line => println(line.toString()))


      println("printing results Partitions")

      queryProcessingResults = QueryProcessing.getQueryProcessingResults()
      //queryProcessingResults.foreach(println)

      println("end printing results")

      queryProcessingResults.iterator

    }

    val tempValuesData = temps.mapPartitionsWithIndex(executeQuery1)

    tempValuesData.foreach(println)

  }
}
object QueryProcessing {

  var queryProcessing = Set[Row]()

  def setQueryProcessingResults(sets: Set[Row]) = {
    queryProcessing = sets
  }

  def getQueryProcessingResults(): Set[Row] = {

    return queryProcessing
  }

}




