package graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.scalatest._

import scala.util.Random

class MaximalIndependentSetTest extends FlatSpec {

  val numTests: Int = 10
  val scale: Int = 10
  val sc = new SparkContext("local", "Maximal Independent Set Test")

  var haveFailed = false

  "An MIS of a random graph" should "be maximum and independent" in {
    1 to numTests foreach { n =>
      if (!haveFailed) {
        val seed = Random.nextInt()
        val rng = new Random(seed)
        val graph = Util.genERGraph(sc, scale * n * n, (_, _) => rng.nextBoolean, 0, (_, _) => null)
        val message = s"Random graph test #${n}: ${graph.vertices.count()} vertices, ${graph.edges.count()} edges, seed = ${seed}"
        test(graph, message)
      }
    }
  }

  "An MIS of a grid graph" should "be maximum and independent" in {
    1 to numTests foreach { n =>
      if (!haveFailed) {
        val graph = GraphGenerators.gridGraph(sc, scale * n, scale * n)
        val message = s"Grid graph test #${n}: ${graph.vertices.count()} vertices, ${graph.edges.count()} edges. "
        test(graph, message)
      }
    }
  }

    "An MIS of a star graph" should "be maximum and independent" in {
    1 to numTests foreach { n =>
      if (!haveFailed) {
        val graph = GraphGenerators.starGraph(sc, scale * n)
        val message = s"Star graph test #${n}: ${graph.vertices.count()} vertices, ${graph.edges.count()} edges. "
        test(graph, message)
      }
    }
  }
  def test (graph: Graph[_, _], message: String): Unit = {
    println(message)
    val misGraph = MaximalIndependentSet(graph)
    println("An MIS with size " + misGraph.vertices.filter(v => v._2).count() + " is found.\n")
    val bugs = MaximalIndependentSet.findInvalidVertices(misGraph, 5)
    if (bugs.length > 0) {
      bugs.foreach(v => println("[Bug] V" + +v._1 + ": " + v._2))
      misGraph.mapTriplets(e => {
        var src = "V" + e.srcId
        var dst = "V" + e.dstId
        if (e.srcAttr) src = "(" + src + ")"
        if (e.dstAttr) dst = "(" + dst + ")"
        src + " <---> " + dst
      }).edges.collect().foreach(e => println(e.attr))
      haveFailed = true
      fail()
      sc.stop()
    } else {
      graph.unpersist()
      misGraph.unpersist()
    }
  }
}
