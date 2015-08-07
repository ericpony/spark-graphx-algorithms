package graphx

import scala.reflect.ClassTag
import scala.util.Random

/**
 * A pregel implementation of a randomized greedy maximal independent set algorithm.
 *
 * Reference:
 * M. Luby. A Simple Parallel Algorithm for the Maximal Independent Set Problem.
 * SIAM Journal on Computing, 15(4), 1986.
 */
object MaximalIndependentSet {

  import org.apache.spark.graphx._

  type MISVertex = (VertexState, Double, Random)
  type VertexState = Int

  /**
   * Remark: the input graph will be treated as an undirected graph.
   */
  def apply[VD: ClassTag] (graph: Graph[VD, _],
                           numIter: Int = Int.MaxValue,
                           isConnected: Boolean = false): Graph[Boolean, _] = {
    val Unknown = 0
    val Tentative = 1
    val Excluded = 2
    val seed = Random.nextLong
    val space = Random.nextInt
    val misGraph = Graph(graph.degrees.map {
      v => (v._1, (Unknown, 1.0 / (2 * v._2), new Random(seed + v._1 * space)))
    }, graph.edges)

    val g = Pregel(misGraph, Tentative)(
      (vid, data, nextState) => (
        if (nextState == Tentative)
          if (data._3.nextDouble < data._2) Tentative else Unknown
        else
          nextState, data._2, data._3),
      e => {
        if (e.srcAttr._1 == Tentative && e.dstAttr._1 == Tentative)
          Iterator((math.max(e.srcId, e.dstId), Excluded))
        else if (e.srcAttr._1 == Tentative && e.dstAttr._1 == Unknown)
          Iterator((e.dstId, Excluded))
        else if (e.srcAttr._1 == Unknown && e.dstAttr._1 == Tentative)
          Iterator((e.srcId, Excluded))
        else if (e.srcAttr._1 == Unknown && e.dstAttr._1 == Unknown)
          Iterator((e.srcId, Tentative), (e.dstId, Tentative))
        else
          Iterator()
      },
      math.max(_, _)).mapVertices {
      (_, data) => data._1 == Tentative
    }
    if (isConnected) g
    else graph.outerJoinVertices(g.vertices) {
      (_, _, opt) => opt.getOrElse(true)
    }
  }
}

object MaximalIndependentSetExample {

  import org.apache.spark.graphx._
  import org.apache.spark.{SparkConf, SparkContext}

  def main (args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("MSF Example"))
    val rng = new Random(12345)
    val numVertices = 50

    // construct an undirected complete graph with `numVertices` vertices
    val vids: List[Int] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[Null]]()) {
        (list, j) => if (j > i && rng.nextBoolean) list :+ Edge(i, j, null) else list
      }
    })
    println("Undirected edges: ")
    edges.cache().collect().foreach(e => println(e.srcId + " <---> " + e.dstId))

    val graph = Graph.fromEdges(edges, 0)

    println("Computing a MSF for a " + numVertices + "-clique...")
    val resGraph = MaximalIndependentSet(graph)

    // a vertex's attribute is the vertex id of its parent node in the MSF
    resGraph.vertices.collect().foreach(v => println("Vertex(" + v._1 + ", " + v._2 + ")"))
  }
}