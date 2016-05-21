import java.io.{PrintWriter, File}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import sparksparql.SparqlContext

import scala.util.Try

/**
  * Created by Bannu on 4/3/2016.
  */
object SampleData {

  def main(args: Array[String]) {
    val t0 = System.currentTimeMillis()
    println(t0)

    val conf = new SparkConf().setAppName("testing").setMaster("local").set("spark.driver.memory", "16g")

    val sc = new SparkContext(conf)
    val sqlconText = new SQLContext(sc)
    val sparqlContext = new SparqlContext(sqlconText)
    val boradCastedSQLContext = sc.broadcast(sqlconText)

    //val data = sc.textFile(,100)

    val data = sc.textFile("src/main/resources/output",100)



    //class for partitioning files
    def createPartitionFiles(index: Int, iterator: Iterator[String]): Iterator[(String)] = {

      val filenameWithExtension = "src/main/resources/indexes/index" + index + ".ttl"
      println("printing file name" + filenameWithExtension)
      val pw = new PrintWriter(new File(filenameWithExtension))
      var i = 0
      var list = Set[String]()
      while (iterator.hasNext) {

        val dat = iterator.next()
        pw.write(dat + "\n")

        i = i + 1
        list = list + (i.toString)
      }
      pw.close()
      list.iterator
    }
    //mapPartitionsWithIndex method
    val mapPartitionWithFileCreation = data.mapPartitionsWithIndex(createPartitionFiles)
    val query ="""prefix skos: <http://www.w3.org/2004/02/skos/core#>
                                        prefix owl:  <http://www.w3.org/2002/07/owl#>
                                        prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#>
                                        prefix xsd: <http://www.w3.org/2001/XMLSchema#>
                                        prefix umls: <http://bioportal.bioontology.org/ontologies/umls/>

                      			select *
                                        WHERE {
                                          ?s a owl:Class .
                                          ?s skos:prefLabel "Ganglia, Autonomic"@eng .
                                          ?s skos:notation ?notation .
                                          ?s umls:hasSTY ?sty .
                                          ?s umls:cui ?cui
                                        }"""
    new QueryProcessing().resultsExtraction("src/main/resources/indexes",mapPartitionWithFileCreation,query,boradCastedSQLContext)
    val t1= System.currentTimeMillis()
    println("total time = "+(t1-t0)+"ms")
  }
}
