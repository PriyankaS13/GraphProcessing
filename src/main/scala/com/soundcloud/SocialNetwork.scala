package com.soundcloud

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, StructType}
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
//import org.apache.log4j.{Level, Logger}
class SocialNetwork (spark_session:SparkSession, deg:Int) extends java.io.Serializable {
//  @transient lazy val logger = Logger.getLogger(this.getClass.getName)
//  logger.setLevel(Level.INFO)
  if (deg <= 0) throw new RuntimeException("Degree can't be negative")

  /**
   * Loads the input data from given path
   * @param file_path
   * @param schema
   * @return
   */
  def loadData(file_path:String,schema:String):DataFrame={
    if (file_path.trim isEmpty) throw new RuntimeException("Invalid Path")
    val dataSchema = DataType.fromJson(schema).asInstanceOf[StructType]
    val edges = spark_session.read.option("delimiter","\\t").schema(dataSchema).csv(file_path)

    edges.select("*").filter(col("src").isNotNull)
  }

  /**
   * computes distance map of paths between every user
   * @param graph
   * @param idList
   * @return
   */
  def computeDistanceMap(graph:GraphFrame, idList:Seq[Any]): DataFrame={
    val paths = graph.shortestPaths.landmarks(idList).run()
    paths
  }

  /**
   * Udf to retrieve lexicographically sorted friends till ith degree
   * @param distance
   * @return
   */
  def retrieveFriend(distance:scala.collection.immutable.Map[String,Int]):String={
    var suggestion = new ListBuffer[String]
    for ((k,v)<-distance){
      if (1 to deg contains v) suggestion += k
    }
    suggestion = suggestion.sorted
    suggestion.mkString("\t")
  }

  /**
   * Retrieves the ith degree friends of all users
   * @param pathMap
   * @param degree
   * @return
   */
  def friendFinder(pathMap:DataFrame,degree:Int):DataFrame={
    val mapudf = udf((d:scala.collection.immutable.Map[String,Int])=>retrieveFriend(d))
    val result = pathMap.withColumn("friends",mapudf(col("distances")))
    result
  }

}


object SocialNetwork{
//  @transient lazy val logger = Logger.getLogger(this.getClass.getName)
//  logger.setLevel(Level.INFO)
  def main(args:Array[String]): Unit ={
    if (args.length <3) {
      println("Incorrect input arguments")
      sys.exit(-1)
    }
    val path = args(0)
    val deg = args(1).toInt
    val outputPath = args(2)
    val sc : SparkSession = SparkSession.builder().appName("SocialNetwork")
      .master("local[*]").getOrCreate()
    val utils = new GraphUtils(sc)
    val social = new SocialNetwork(sc, deg)
    val schema = ConfigFactory.load().getString("com.soundcloud.edgeSchema")
//    logger.info("About to load the edge list")
    var edges = social.loadData(path,schema)
    val vertices = utils.computeVertices(edges)
//    logger.info("Generating bidirectional edges")
    edges = utils.biDirectionalTransformer(edges,"src","dst")
    val graph = utils.generategraph(vertices,edges)
//    logger.info("Generated graphframe from input data")
    var ids = graph.vertices.select("id").collect().map(_(0)).toList.toSeq
//    logger.info("About to generate distance map between vertex")
    val paths = social.computeDistanceMap(graph,ids)
    val result = social.friendFinder(paths,deg)
//    logger.info("Retrieved the friends of nth degree")
    result.select(col("id").alias("user"),col("friends"))
      .orderBy("id").repartition(1).write.mode("Overwrite").csv(outputPath)
//    logger.info("Result saved in csv format")
  }

}
