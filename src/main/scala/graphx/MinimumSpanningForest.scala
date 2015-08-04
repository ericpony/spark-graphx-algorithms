package graphx

import org.apache.spark.graphx._

import scala.reflect.ClassTag

object MinimumSpanningForest {
  def run[VD: ClassTag] (graph: Graph[VD, Double], numIter: Int): Iterator[(VertexId, VertexId)] = {

    // the graph we update with final SCC ids, and the graph we return at the end
    var msfGraph = graph.mapVertices { case (vid, _) => vid}
    // graph we are going to work with in our iterations
    var msfWorkGraph = graph.mapVertices { case (vid, _) => (vid, false)}.cache()
    var numVertices = msfWorkGraph.numVertices
    var iter = 0
    while (msfWorkGraph.numVertices > 0 && iter < numIter) {
      // record the number of supersteps
      iter += 1
      // each vertex attribute is the id of the vertex at the minimal outgoing edge
      val minEdgeGraph = msfGraph.aggregateMessages[(VertexId, Double)](
      ctx => ctx.sendToSrc((ctx.dstId, ctx.attr)), { case ((v1, w1), (v2, w2)) => if (w1 > w2) (v2, w2) else (v1, w1)})
      // each vertex attribute containts (pointerVertexId, isSuperVertex, isFirstStep)
      var superVertexGraph = Graph(minEdgeGraph.mapValues[(VertexId, Boolean, Boolean)](v => (v._1, false, true)), msfWorkGraph.edges)
      superVertexGraph = superVertexGraph.pregel(-1: VertexId)(
        (vid, attr, msg) => {
          if (attr._3) {
            if (msg == attr._1)
              (math.min(msg, vid), true, false)
            else
              (vid, false, false)
          } else
            (msg, true, false)
        },
        e => {
          if (e.srcAttr._3) // initialization
            return Iterator((e.srcAttr._1, e.srcId))
          if (e.dstAttr._2) // write the id of the superVertex to the srcVertex
            return Iterator((e.srcId, e.dstAttr._1))
          else
            Iterator()
        },
        (vid1, vid2) => math.min(vid1, vid2)
      )
    }
    msfGraph
  }
}
