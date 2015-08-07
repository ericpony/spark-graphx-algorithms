package graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.reflect.ClassTag

object MyGraphGenerators {
  def completeGraph[VD: ClassTag, ED:ClassTag] (sc: SparkContext,
                             numVertices: Int,
                             vAttr: VD,
                             eAttr: (VertexId, VertexId) => ED): Graph[VD, ED] = {
    // construct an undirected complete graph with `numVertices` vertices
    val vids: List[Int] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[ED]]()) {
        (list, j) => if (j > i) list :+ Edge(i, j, eAttr(i, j)) else list
      }
    })
    Graph.fromEdges(edges, vAttr)
  }

  def randomGraph[VD:ClassTag, ED:ClassTag] (sc: SparkContext,
                           numVertices: Int,
                           prob: (VertexId, VertexId) => Boolean,
                           vAttr: VD,
                           eAttr: (VertexId, VertexId) => ED): Graph[VD, ED] = {
    // construct an undirected complete graph with `numVertices` vertices
    val vids: List[Int] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[ED]]()) {
        (list, j) => if (j > i && prob(i, j)) list :+ Edge(i, j, eAttr(i, j)) else list
      }
    })
    Graph.fromEdges(edges, vAttr)
  }
}
