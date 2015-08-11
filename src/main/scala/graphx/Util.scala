package graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.reflect.ClassTag

object Util {

  /**
   * Remove isolated vertices from the input graph.
   *
   * @param graph graph to remove isolated vertices
   * @tparam VD type of vertex attribute
   * @tparam ED type of edge attribute
   * @return a graph without isolated vertices
   */
  def removeIsolatedVertices[VD: ClassTag, ED: ClassTag]
  (graph: Graph[VD, ED]): Graph[VD, ED] = {
    graph.filter[(VD, Boolean), ED](
      g => g.outerJoinVertices[Int, (VD, Boolean)](graph.degrees) {
        (_, attr, deg) => (attr, deg.isDefined)
      },
      vpred = (v, attr) => attr._2
    )
  }

  /**
   * Update isolated vertices in the input graph using vprog.
   *
   * @param graph graph to mark isolated vertices
   * @param vprog a function to update attribute for each isolated vertex
   * @tparam VD type of vertex attribute
   * @tparam ED type of edge attribute
   * @return a graph where attributes of isolated vertices are updated
   */
  def mapIsolatedVertices[VD: ClassTag, ED: ClassTag]
  (graph: Graph[VD, ED],
   vprog: (VertexId, VD) => VD): Graph[VD, ED] = {
    graph.outerJoinVertices[Int, (VD, Boolean)](graph.degrees) {
      (_, attr, deg) => (attr, deg.isDefined)
    }.mapVertices {
      (vid, attr) => if (attr._2) attr._1 else vprog(vid, attr._1)
    }
  }

  /**
   * Generate a complete graph, i.e., a graph where all vertices are adjacent to each other.
   * @param sc Spark Context
   * @param numVertices number of vertices in generated graph
   * @param vAttr default value of vertex attribute
   * @param eAttr a function to compute attribute for each edge
   * @tparam VD type of vertex attribute
   * @tparam ED type of edge attribute
   * @return Graph object
   */
  def genCompleteGraph[VD: ClassTag, ED: ClassTag] (sc: SparkContext,
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

  /**
   * Generate an generalized Erdős–Rényi (ER) random graph.
   *
   * @param sc Spark Context
   * @param numVertices number of vertices in generated graph
   * @param choose a function to decide the presence of edge
   * @param vAttr default value of vertex attribute
   * @param eAttr a function to compute attribute for each edge
   * @tparam VD type of vertex attribute
   * @tparam ED type of edge attribute
   * @return Graph object
   */
  def genERGraph[VD: ClassTag, ED: ClassTag] (sc: SparkContext,
                                              numVertices: Int,
                                              choose: (VertexId, VertexId) => Boolean,
                                              vAttr: VD,
                                              eAttr: (VertexId, VertexId) => ED): Graph[VD, ED] = {
    // construct an undirected complete graph with `numVertices` vertices
    val vids: List[Int] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[ED]]()) {
        (list, j) => if (j > i && choose(i, j)) list :+ Edge(i, j, eAttr(i, j)) else list
      }
    })
    Graph.fromEdges(edges, vAttr)
  }
}
