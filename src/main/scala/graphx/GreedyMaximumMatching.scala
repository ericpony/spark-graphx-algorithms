package graphx

import graphx.Types._
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * A Pregel implementation of a greedy maximum matching algorithm
 * with 0.5 approximation ratio.
 *
 * Author:
 * ericpony (https://github.com/ericpony/graphx-examples)
 *
 * Reference:
 * Hoepman, Jaap-Henk. "Simple distributed weighted matchings."
 * arXiv preprint cs/0410047 (2004).
 */
object GreedyMaximumMatching {

  /**
   * Remark: the input graph will be treated as an undirected graph.
   */
  def apply[VD : ClassTag] (
    graph : Graph[VD, EdgeWeight],
    maxNumIterations : Int = Int.MaxValue,
    isConnected : Boolean = false
    ) : Graph[VertexId, EdgeWeight] = {
    val UNMATCHED = 0
    val WAITING = 1
    val MATCHED = 2
    val g : Graph[(Int, VertexId), EdgeWeight] =
      (if (isConnected) graph else Utility.removeIsolatedVertices(graph))
        .mapVertices((_, _) => (-1, 0L))

    Pregel(g, (0L, 0D))(
      (id, state, msg) => state match {
        case (UNMATCHED, _)                    => (WAITING, msg._1)
        case (WAITING, mate) if (msg._1 == id) => (MATCHED, mate)
        case _                                 => (UNMATCHED, 0L)
      },
      e => {
        var ret = List[(VertexId, (VertexId, EdgeWeight))]()
        if (e.srcAttr._1 != MATCHED && e.dstAttr._1 != MATCHED) {
          if (e.srcAttr._1 == WAITING)
            ret = (e.srcId, (e.dstAttr._2, e.attr)) :: ret
          if (e.dstAttr._1 == WAITING)
            ret = (e.dstId, (e.srcAttr._2, e.attr)) :: ret
          if (e.srcAttr._1 == UNMATCHED)
            ret = (e.srcId, (e.dstId, e.attr)) :: ret
          if (e.dstAttr._1 == UNMATCHED)
            ret = (e.dstId, (e.srcId, e.attr)) :: ret
        }
        ret.toIterator
      },
      (m1, m2) =>
        if (m1._2 == m2._2) {
          if (m1._1 > m2._1) m1 else m2
        } else {
          if (m1._2 > m2._2) m1 else m2
        }
    ).mapVertices((_, state) => state._2)
  }
}