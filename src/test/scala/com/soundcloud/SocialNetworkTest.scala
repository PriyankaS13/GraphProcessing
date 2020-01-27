package com.soundcloud

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}

class SocialNetworkTest extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll {
  val path = "/Users/greddy/Documents/workspace/socialnetwork/src/soundcloud.txt"
  val schema = ConfigFactory.load().getString("com.soundcloud.edgeSchema")
  val ss = SparkSession.builder().appName("SocialNetworkTest")
    .master("local[*]").getOrCreate()
  val utils = new GraphUtils(ss)
  val deg = 2
  val social = new SocialNetwork(ss, deg)

  override def afterAll() {
    ss.close()
  }
  "dataframe schema" should "match defined schema" in {
    val dataSchema = DataType.fromJson(schema).asInstanceOf[StructType]
    assert(social.loadData(path,schema).schema === dataSchema )
  }

  "degree" should "accept only value greater than 0" in {
    assertThrows[RuntimeException] {
      val socialTest = new SocialNetwork(ss, -1)
    }
  }

  "path" should "not be blank" in {
    assertThrows[RuntimeException] {
      social.loadData("", schema)
    }
  }

  "null records" should "discarded" in {
    val path="/Users/greddy/Documents/workspace/socialnetwork/src/soundcloud_test_data.txt"
    assert(social.loadData(path, schema).filter(col("src").isNull).count()===0)

  }

  "brendan 2nd degree friends" should "kim,omid,torsten" in {
    var edges = social.loadData(path, schema)
    val vertices = utils.computeVertices(edges)
    edges = utils.biDirectionalTransformer(edges,"src","dst")
    val graph = utils.generategraph(vertices,edges)
    graph.cache()
    var ids = graph.vertices.select("id").collect().map(_(0)).toList.toSeq
    val paths = social.computeDistanceMap(graph,ids)
    val result = social.friendFinder(paths,deg)
    assert(result.filter(col("id")==="brendan").select(col("friends"))
     .collect().map(_(0)).toList.mkString("\t") === "kim\tomid\ttorsten")

  }
}
