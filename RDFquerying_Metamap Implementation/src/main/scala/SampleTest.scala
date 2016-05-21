
import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import sparksparql.SparqlContext
object SampleTest {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("Testing SpaqrlConnector").setMaster("local")

    //val dbpediaEndpoint = "src/main/resources/sampledataset.nt"
    val sc = new SparkContext(conf)
    val sqlconText = new SQLContext(sc)
    val sparqlContext = new SparqlContext(sqlconText)

    //val datasetsRDD = sc.textFile("src/main/resources/sampledataset.nt").saveAsTextFile("src/main/resources/index")

    //    var pathString = "src/main/resources/index/part-00000"
    //    new File("/part-00000").renameTo(new File(pathString+".nt"))
    val dbpediaEndpoint = "mem:dataset=src/main/resources/sample.ttl"
    //val dbpediaEndpoint = "mem:dataset=src/main/resources/output/sample.ttl"
    //val dbpediaEndpoint1 = "mem:dataset=src/main/resources/output/AOT2003.ttl"

    //val service = "http://sparql.bioontology.org/sparql"
    val query =
      """prefix skos: <http://www.w3.org/2004/02/skos/core#>
                                        prefix owl:  <http://www.w3.org/2002/07/owl#>
                                        prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
                                        prefix xsd: <http://www.w3.org/2001/XMLSchema#>
                                        prefix umls: <http://bioportal.bioontology.org/ontologies/umls/>

                      				          SELECT *
                                        WHERE {
                                          ?s a owl:Class .
                                          ?s skos:notation ?notation .
                                          ?s skos:prefLabel ?label
                                        }"""
    //        val query =  """PREFIX omv: <http://omv.ontoware.org/2005/05/ontology#>
    //
    //                     SELECT ?ont ?name ?acr
    //                     WHERE {
    //                     	?ont a omv:Ontology .
    //                     	?ont omv:acronym ?acr .
    //                     	?ont omv:name ?name .
    //                     }"""
    val dataFrame = sparqlContext.sparqlQuery(dbpediaEndpoint, query)

    //val dataTest = sqlconText.sparqlQuery(service,query)
    dataFrame.show(100000, false)
    //dataTest.show(100000,false)
//    def onFlyApproach(inputPath: String, sc: SparkContext, query: String, broadcast: Broadcast[SQLContext], sparqlContext: SparqlContext) = {
//
//      println("query:" + query)
//
//      val sparqlContext = broadcast.value
//
//      val temps = sc.textFile(inputPath + "/indexes/tempdata")
//
//      def executeQuery(index: Int, iterator: Iterator[(String)]): Iterator[(Row)] = {
//
//        val dbpediaEndpoint = "mem:dataset=" + inputPath + "/index" + index + ".nt"
//
//        println("dbpediaEndpoint:" + dbpediaEndpoint)
//
//        if (sparqlContext == null) {
//          println("hello how are you..?inside sqlcontext")
//        }
//        if (dbpediaEndpoint == null) {
//          println("hello how are you..?inside dbpediaendpoint")
//        }
//        if (query == null) {
//          println("hello how are you..? inside query")
//        }
//
//        val dataFrame = sparqlContext.sparqlQuery(dbpediaEndpoint, query)
//
//
//
//        var result = Set[(Row)]()
//
//        dataFrame.show()
//
//        dataFrame.rdd.map(line => {
//          result = result + (line)
//        })
//
//        result.iterator
//      }
//
//      val tempValuesData = temps.mapPartitionsWithIndex(executeQuery)
//
//      tempValuesData.map(line => {
//        println(line.toString())
//      }).foreach(println)
//
//    }
  }
}