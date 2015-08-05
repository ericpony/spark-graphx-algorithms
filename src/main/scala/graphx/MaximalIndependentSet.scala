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

  type MISVertex = (Boolean, Random)

  private def initGraph[VD: ClassTag] (graph: Graph[VD, _], isConnected: Boolean): Graph[MISVertex, _] = {

    val seed = Random.nextLong
    val space = Random.nextInt

    if (isConnected)
      graph.mapVertices { (vid, _) => (false, new Random(seed + vid * space))}
    else {
      // identify the isolated vertices
      val g = graph.outerJoinVertices[Int, Boolean](graph.outDegrees) {
        (vid, _, degreeOpt) => !degreeOpt.isDefined
      }.outerJoinVertices(graph.inDegrees) {
        (vid, mark, degreeOpt) => !degreeOpt.isDefined && mark
      }.vertices.filter(_._2)

      // let the isolated vertices point to themselves
      graph.outerJoinVertices(g) { (vid, data, opt) => (!opt.isDefined, new Random(seed + vid * space))}
    }
  }

  /**
   * Remark: the input graph will be treated as an undirected graph.
   */
  def run (graph: Graph[_, _], numIter: Int = Int.MaxValue, isConnected: Boolean = false): Graph[Boolean, _] = {
    val misGraph = initGraph(graph, isConnected)

    misGraph.mapVertices((_, data) => data._1)
  }

}
