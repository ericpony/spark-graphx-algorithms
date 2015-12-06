package graphx

import org.apache.spark.graphx._
import scala.reflect.ClassTag

/**
  * Created by ericpony on 2015/12/05.
  */
object BreadthFirstSearch {

  final case class EdgeTripletAdapter[VD: ClassTag, ED: ClassTag]
  (e: EdgeTriplet[(Long, VD, ), ED]) {
    def srcId = e.srcId
    def srcAttr = e.srcAttr._2
    def dstId = e.dstId
    def dstAttr = e.dstAttr._2
    def attr = e.attr
  }

  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag]
  (graph: Graph[VD, ED],
   sourceId: VertexId,
   initMsg: A,
   vprog: (VertexId, VD, A) => VD,
   sendMsg: EdgeTripletAdapter[VD, ED] => Option[A]): Graph[VD, ED] = {

    val dfsGraph = graph.mapVertices { (vid, data) =>
      if (vid == sourceId)
        (0L, vprog(vid, data, initMsg))
      else
        (Long.MaxValue, data)
    }
    Pregel(dfsGraph, (Long.MaxValue, initMsg), Int.MaxValue, EdgeDirection.Out)(
      (vid, data, msg) => {
        if (data._1 > msg._1) (msg._1, vprog(vid, data._2, msg._2)) else data
      },
      e => {
        if (e.srcAttr._1 + 1 < e.dstAttr._1) {
          val msg = sendMsg(EdgeTripletAdapter(e))
          if (msg.isDefined)
            Iterator((e.dstId, (e.srcAttr._1 + 1, msg.get)))
          else
            Iterator()
        }
        Iterator()
      },
      (d1, d2) => {
        if (d1._1 < d2._1) d1 else d2
      }
    ).mapVertices({ case (_, (_, data)) => data })
  }
}

object BipartiteGraphColoring {
  /**
    * When the provided graph is bipartite, returns a graph with a proper two-coloring in {-1, 1}.
    * Otherwise, return a graph colored by 0.
    */
  def apply[VD: ClassTag, ED: ClassTag]
  (graph: Graph[VD, ED]): Graph[(Int, VD), ED] = {
    var colorG: Graph[(Int, VD), ED] = graph.mapVertices((vid, data) => (if (vid == 0) 1 else 0, data))
    colorG = BreadthFirstSearch[(Int, VD), ED, Int](colorG, 1L, 0,
      (vid, v, c) => if (c == v._1) (0, v._2) else v,
      e => {
        if (e.dstAttr._1 != 0 && e.srcAttr._1 != 0)
          Some(0 - e.srcAttr._1)
        else
          None
      }
    )
    Pregel[(Int, VD), ED, Boolean](colorG, false)(
      (vid, v, erase) => (if (erase) 0 else v._1, v._2),
      e => {
        if (e.srcAttr._1 == e.dstAttr._1)
          Iterator()
        else if (e.srcAttr._1 == 0)
          Iterator((e.dstId, true))
        else if (e.dstAttr._1 == 0)
          Iterator((e.srcId, true))
        else
          Iterator() // unreachable
      },
      (e1, e2) => e1)
  }
}
