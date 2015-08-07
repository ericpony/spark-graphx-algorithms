package graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.scalatest._

import scala.util.Random

class MaximalIndependentSetTest extends FlatSpec {

  val sc = new SparkContext("local", "Maximal Independent Set Test")

  def checkMIS (graph: Graph[Boolean, _]) = {
    // for each vertex, count the number of selected neighbors
    graph.outerJoinVertices(graph.aggregateMessages[Long](
      ctx => {
        if (ctx.srcAttr) ctx.sendToDst(1)
        if (ctx.dstAttr) ctx.sendToDst(1)
      },
      _ + _
    )) {
      (vid, selected, opt) => {
        Predef.assert(!selected && !opt.isDefined, "isolated vertex not selected")
        Predef.assert(selected && opt.getOrElse(0L) > 0, "not independent")
        Predef.assert(!selected && opt.get == 0, "not maximal")
      }
    }.unpersist()
  }

  "An MIS of a random graph" should "be maximum and independent" in {
    val rng = new Random(12345)
    1 to 10 foreach { n =>
      val graph = MyGraphGenerators.randomGraph(sc, 10 * n, (_, _) => rng.nextBoolean, 0, (_, _) => null)
      val misGraph = MaximalIndependentSet(graph)
      checkMIS(misGraph)
      graph.unpersist()
      misGraph.unpersist()
    }
  }

  "An MIS of a grid graph" should "be maximum and independent" in {
    1 to 10 foreach { n =>
      val graph = GraphGenerators.gridGraph(sc, 10 * n, 10 * n)
      val misGraph = MaximalIndependentSet(graph)
      checkMIS(misGraph)
      graph.unpersist()
      misGraph.unpersist()
    }
  }
}