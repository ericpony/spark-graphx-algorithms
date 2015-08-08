package graphx

import scala.reflect.ClassTag
import scala.util.Random

/**
 * A pregel implementation of a minimum spanning forest algorithm.
 *
 * Reference:
 * S. Chung and A. Condon. Parallel Implementation of Boruvka’s Minimum Spanning Tree Algorithm.
 * In Proceedings of the International Parallel Processing Symposium, 1996.
 */
object MinimumSpanningForest extends Logger {

  import graphx.Types._
  import org.apache.spark.graphx._

  /**
   * Remark: the input graph will be treated as an undirected graph.
   */
  def apply[VD: ClassTag] (graph: Graph[VD, EdgeWeight],
                           numIter: Int = Int.MaxValue,
                           isConnected: Boolean = false): Graph[VertexId, EdgeWeight] = {

    // a vertex's attribute is the id of the other vertex at its minimal incident edge
    val vRDD = graph.aggregateMessages[(VertexId, EdgeWeight)](
    ctx => ctx.sendToSrc((ctx.dstId, ctx.attr)), {
      case ((v1, w1), (v2, w2)) =>
        if (w1 > w2) (v2, w2) else (v1, w1)
    })

    info("=== before identifying super-vertices ===")
    vRDD.collect().foreach(v => info("Vertex(" + v._1 + ", (" + v._2._1 + ", " + v._2._2 + "))"))
    info("=====")

    // each vertex attribute containts the vertex id of its parent node in a conjoined tree
    val superVertexGraph = Graph(vRDD.mapValues[VertexId]((v: (VertexId, EdgeWeight)) => v._1), graph.edges)

    info("=== after identifying super-vertices ===")
    superVertexGraph.vertices.collect().foreach(v => info("Vertex(" + v._1 + ", " + v._2 + ")"))
    info("=====")

    // identify the super vertex in each tree
    val msfGraph = Graph(superVertexGraph.aggregateMessages[VertexId](
      ctx => {
        if (ctx.dstId == ctx.srcAttr && ctx.dstAttr == ctx.srcId) {
          // detect a 2-cycle
          // let the super-vertex point to itself
          ctx.sendToDst(if (ctx.dstId < ctx.srcId) ctx.dstId else ctx.dstAttr)
          ctx.sendToSrc(if (ctx.dstId < ctx.srcId) ctx.srcAttr else ctx.srcId)
        } else {
          // let the vertices keep the current pointers
          ctx.sendToDst(ctx.dstAttr)
          ctx.sendToSrc(ctx.srcAttr)
        }
      },
      (vid1, vid2) => math.min(vid1, vid2)
    ), graph.edges)

    if(isConnected) msfGraph
    else graph.outerJoinVertices(msfGraph.vertices) {
      (vid, data, opt) => opt.getOrElse(vid)
    }
  }

  private def trimGraph[VD: ClassTag] (graph: Graph[VD, EdgeWeight]): Graph[VertexId, EdgeWeight] = {

    // identify the isolated vertices
    val g = graph.outerJoinVertices[Int, Boolean](graph.outDegrees) {
      (vid, _, degreeOpt) => !degreeOpt.isDefined
    }.outerJoinVertices(graph.inDegrees) {
      (vid, mark, degreeOpt) => !degreeOpt.isDefined && mark
    }.vertices.filter(_._2)

    // let the isolated vertices point to themselves
    graph.outerJoinVertices(g) { (vid, data, opt) => if (opt.isDefined) 0 else vid}
  }
}

object MinimumSpanningForestExample {

  import graphx.Types._
  import org.apache.spark.graphx._
  import org.apache.spark.{SparkConf, SparkContext}

  def main (args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("MSF Example"))
    val rng = new Random(12345)
    val numVertices = 10

    // construct an undirected complete graph with `numVertices` vertices
    val vids: List[Int] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[EdgeWeight]]()) {
        (list, j) => {
          val w = rng.nextInt(10) * 1.0
          if (j > i) list :+ Edge(i, j, w)
          else list
        }
      }
    })
    println("Undirected edges: ")
    edges.cache().collect().foreach(e => println(e.srcId + " <-- " + e.attr + " --> " + e.dstId))

    val graph = Graph.fromEdges(edges, 0)

    println("Computing a MSF for a " + numVertices + "-clique...")
    val resGraph = MinimumSpanningForest(graph)

    // a vertex's attribute is the vertex id of its parent node in the MSF
    resGraph.vertices.collect().foreach(v => println("Vertex(" + v._1 + ", " + v._2 + ")"))
  }
}