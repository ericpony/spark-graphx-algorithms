package graphx

import scala.reflect.ClassTag
import org.apache.spark.graphx._

object MinimumSpanningTree {

  type EdgeType = ((VertexId, VertexId), Double)

  def minSpanningTree[VD : ClassTag] (g : Graph[VD, Double]) = {

    // We maintain a Boolean edge attribute to indicate whether an edge is
    // selected to construct the spanning tree. Note that the selected edges
    // form a forest before the tree construction process is complete.
    var g2 = g.mapEdges(e => (e.attr, false))

    // add an edge to the forest in each iteration
    // (a forest is a spanning tree iff it contains n - 1 edges)
    for (i <- 1L to g.vertices.count - 1) {

      // select the edges spanned by the current forest
      val unavailableEdges =
      // join the forest so that we have more information to do selection
        g2.outerJoinVertices(
          // extract the forest from g2
          g2.subgraph(_.attr._2)
          // two vertices have the same cid iff they are in the same tree
          .connectedComponents.vertices
        )((vid, vd, cid) => (vd, cid))
        .subgraph(et =>
          // an edge is spanned iff both of its endpoints are contained in the same tree
          (et.srcAttr._2, et.dstAttr._2) match {
            case (Some(c1), Some(c2)) => c1 == c2
            case _                    => false
          }
        )
        .edges
        // convert edges to custom edge type
        .map(e => ((e.srcId, e.dstId), e.attr))

      // find the smallest edge from the available edges
      val smallestEdge =
        g2.edges
        // convert edges to custom edge type
        .map(e => ((e.srcId, e.dstId), e.attr))
        // join the unavailable edges so that we have more information to do filtering
        .leftOuterJoin(unavailableEdges)
        // an edge is available iff it is neither included in nor spanned by the forest
        .filter(x => !x._2._1._2 && x._2._2.isEmpty)
        // convert edges to custom edge type
        .map(x => (x._1, x._2._1._1))
        // select the minimal edge from the available edges
        .min()(new Ordering[EdgeType]() {
          override def compare (a : EdgeType, b : EdgeType) = {
            val r = Ordering[Double].compare(a._2, b._2)
            if (r != 0) r
            else // make the result deterministic
              Ordering[Long].compare(a._1._1, b._1._1)
          }
        })

      // add the smallest edge to the forest
      g2 = g2.mapTriplets(et => (et.attr._1,
        et.attr._2 || (et.srcId == smallestEdge._1._1 && et.dstId == smallestEdge._1._2)))
    }
    // remove the augmented attribute
    g2.subgraph(_.attr._2).mapEdges(_.attr._1)
  }
  //print(minSpanningTree(myGraph).triplets.map(et => (et.srcAttr, et.dstAttr)).collect)
}
