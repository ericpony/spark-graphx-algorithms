package graphx

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/** Strongly connected components algorithm implementation. */
object StronglyConnectedComponentsV2 {

  /**
   * Compute the strongly connected component (SCC) of each vertex and return a graph with the
   * vertex value containing the lowest vertex id in the SCC containing that vertex.
   *
   * Source: https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/StronglyConnectedComponents.scala
   */
  def apply[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED], numIter: Int): Graph[VertexId, ED] = {

    // the graph we update with final SCC ids, and the graph we return at the end
    var sccGraph = graph.mapVertices { case (vid, _) => vid }
    // graph we are going to work with in our iterations
    var sccWorkGraph = graph.mapVertices { case (vid, _) => (vid, false) }.cache()
    var numVertices = sccWorkGraph.numVertices
    var iter = 0
    while (sccWorkGraph.numVertices > 0 && iter < numIter) {
      // record the number of supersteps
      iter += 1

      // remove trivial SCCs from both sccGraph and sccWorkGraph
      do {
        numVertices = sccWorkGraph.numVertices

        // identify the vertices without incoming or outgoing edges
        sccWorkGraph = sccWorkGraph.outerJoinVertices(
          sccWorkGraph.outDegrees.fullOuterJoin(sccWorkGraph.inDegrees).mapValues {
            case ((opt1, opt2)) => !opt1.isDefined || !opt2.isDefined
          }
        ) {
          (vid, data, isFinal) => (vid, isFinal.isDefined)
        }.cache()

        // get the colors of all trivial SCCs
        val finalVertices = sccWorkGraph.vertices
          .filter { case (vid, (scc, isFinal)) => isFinal }
          .mapValues { (vid, data) => data._1 }

        // write the colors of the trivial SCCs to sccGraph
        sccGraph = sccGraph.outerJoinVertices(finalVertices) {
          (vid, scc, opt) => opt.getOrElse(scc)
        }

        // only keep vertices that are not trivial SCCs
        sccWorkGraph = sccWorkGraph.subgraph(vpred = (vid, data) => !data._2).cache()

      } while (sccWorkGraph.numVertices < numVertices) // repeat until no trivial SCCs are left

      // reset the color of each vertex to itself
      sccWorkGraph = sccWorkGraph.mapVertices { case (vid, (color, isFinal)) => (vid, isFinal) }

      // collect min of all my neighbor's scc values, update if it's smaller than mine
      // then notify any neighbors with scc values larger than mine
      sccWorkGraph = Pregel[(VertexId, Boolean), ED, VertexId](
        sccWorkGraph, Long.MaxValue, activeDirection = EdgeDirection.Out)(
          (vid, myScc, neighborScc) => (math.min(myScc._1, neighborScc), myScc._2),
          e => {
            if (e.srcAttr._1 < e.dstAttr._1) {
              Iterator((e.dstId, e.srcAttr._1))
            } else {
              Iterator()
            }
          },
          (vid1, vid2) => math.min(vid1, vid2))

      // start at root of SCCs. Traverse values in reverse, notify all my neighbors
      // do not propagate if colors do not match!
      sccWorkGraph = Pregel[(VertexId, Boolean), ED, Boolean](
        sccWorkGraph, false, activeDirection = EdgeDirection.In)(
          // vertex is final if it is the root of a color
          // or it has the same color as a neighbor that is final
          (vid, myScc, existsSameColorFinalNeighbor) => {
            val isColorRoot = vid == myScc._1
            (myScc._1, myScc._2 || isColorRoot || existsSameColorFinalNeighbor)
          },
          // activate neighbor if they are not final, you are, and you have the same color
          e => {
            val sameColor = e.dstAttr._1 == e.srcAttr._1
            val onlyDstIsFinal = e.dstAttr._2 && !e.srcAttr._2
            if (sameColor && onlyDstIsFinal) {
              Iterator((e.srcId, true))
            } else {
              Iterator()
            }
          },
          (final1, final2) => final1 || final2)
    } // end of main loop
    sccGraph
  }
}