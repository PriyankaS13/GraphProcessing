package com.soundcloud
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame

class GraphUtils(ss: SparkSession) extends java.io.Serializable {
  import ss.implicits._

  /**
   * Generates bidirectional edges
   * @param edge
   * @param srcCol
   * @param dstCol
   * @return
   */
  def biDirectionalTransformer(edge: DataFrame, srcCol:String, dstCol:String):DataFrame={

      var edgeModified = edge.select(col(srcCol),col(dstCol))
      .union(edge.select(col(dstCol),col(srcCol)))

    edgeModified = edgeModified.withColumn("relationship",lit("friend"))
    edgeModified
  }

  /**
   * creates the vertices data frame
   * @param edge
   * @return
   */
  def computeVertices(edge:DataFrame):DataFrame={
    var vertices = edge.select(edge("src")).union(edge.select(edge("dst"))).distinct()
                        .select(col("src").alias("id")
                          ,col("src").alias("name"))
    vertices
  }

  /**
   * Generates a graph frame from given edges and vertices
   * @param vertice
   * @param edges
   * @return
   */

  def generategraph(vertice:DataFrame,edges:DataFrame):GraphFrame={
    val graph= GraphFrame (vertice,edges)
    graph
  }
}
