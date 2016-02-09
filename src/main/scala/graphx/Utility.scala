package graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeRDD, Edge, Graph, VertexId}

import scala.reflect.ClassTag

object Utility {

  /**
   * Remove isolated vertices from the input graph.
   *
   * @param graph graph to remove isolated vertices
   * @tparam VD type of vertex attribute
   * @tparam ED type of edge attribute
   * @return a graph without isolated vertices
   */
  def removeIsolatedVertices[VD : ClassTag, ED : ClassTag] (
    graph : Graph[VD, ED]
    ) : Graph[VD, ED] = {
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
  def mapIsolatedVertices[VD : ClassTag, ED : ClassTag] (
    graph : Graph[VD, ED],
    vprog : (VertexId, VD) => VD
    ) : Graph[VD, ED] = {
    graph.outerJoinVertices[Int, (VD, Boolean)](graph.degrees) {
      (_, attr, deg) => (attr, deg.isDefined)
    }.mapVertices {
      (vid, attr) => if (attr._2) attr._1 else vprog(vid, attr._1)
    }
  }

  /**
   * Compute the cograph of a undirected graph.
   * @param g a undirected graph
   * @tparam VD1 type of vertex attribute
   * @tparam VD2 type of edge attribute
   * @return the cograph of g
   */
  def cograph[VD1 : ClassTag, VD2 : ClassTag] (
    g : Graph[VD1, VertexId],
    vAttr : VD2,
    eAttr : (VertexId, VertexId) => VD1
    ) : Graph[VD2, VD1] = {
    val edges = g.aggregateMessages[(VD1, List[VertexId])](
      e => {
        if (e.srcId > e.dstId)
          e.sendToSrc((e.srcAttr, List(e.attr)))
        else
          e.sendToDst((e.dstAttr, List(e.attr)))
      },
      (e1, e2) => (e1._1, e1._2 ++ e2._2)
    ).mapValues((_, vids) => vids._2.flatMap {
      i => vids._2.foldLeft(List[Edge[VD1]]()) {
        (list, j) => Edge(i, j, eAttr(i, j)) :: list
      }
    }).flatMap(_._2)
    Graph.fromEdges(edges, vAttr)
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
  def genCompleteGraph[VD : ClassTag, ED : ClassTag] (
    sc : SparkContext,
    numVertices : Int,
    vAttr : VD,
    eAttr : (VertexId, VertexId) => ED
    ) : Graph[VD, ED] = {
    // construct an undirected complete graph with `numVertices` vertices
    val vids : List[Int] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[ED]]()) {
        (list, j) => if (j > i) Edge(i, j, eAttr(i, j)) :: list else list
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
  def genERGraph[VD : ClassTag, ED : ClassTag] (
    sc : SparkContext,
    numVertices : Int,
    choose : (VertexId, VertexId) => Boolean,
    vAttr : VD,
    eAttr : (VertexId, VertexId) => ED
    ) : Graph[VD, ED] = {
    // construct an undirected complete graph with `numVertices` vertices
    val vids : List[Int] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[ED]]()) {
        (list, j) =>
          if (j > i && choose(i, j)) Edge(i, j, eAttr(i, j)) :: list
          else list
      }
    })
    Graph.fromEdges(edges, vAttr)
  }
}
