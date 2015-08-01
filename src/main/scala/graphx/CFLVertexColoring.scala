package graphx

import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object CFLVertexColoring {

  type Color = Int
  type Palette = (Color, List[Double], Boolean, Long)

  private def sampleColor (dist: List[Double], rnd: Double): Int = {
    dist.foldLeft((1, 0.0)) {
      case ((color, mass), weight) => {
        val m = mass + weight
        (if (m < rnd) color + 1 else color, m)
      }
    }._1
  }

  def run (graph: Graph[Color, _], beta: Double, maxNumColors: Int): Graph[Color, _] = {

    val initColorDist = (1 to maxNumColors).toList.map(_ => 1.0 / maxNumColors)
    val seed = Random.nextLong
    val distGraph = graph.mapVertices((id, attr) => (attr, initColorDist, true, seed + id))

    def sendMessage (edge: EdgeTriplet[Palette, _]): Iterator[(VertexId, Boolean)] = {
      if (edge.srcAttr._1 == edge.dstAttr._1)
        return Iterator((edge.srcId, true))
      if (edge.srcAttr._3)
        return Iterator((edge.srcId, false))
      Iterator.empty
    }
    def vprog (id: VertexId, attr: Palette, active: Boolean): Palette = {
      val color = attr._1
      val dist  = attr._2
      val seed  = attr._4
      val new_dist = dist.foldLeft((1, List[Double]())) {
        case ((i, list), weight) => (i + 1,
          if (active)
            list :+ (weight * (1 - beta) + (if (color == i) 0.0 else beta / (maxNumColors - 1)))
          else
            list :+ (if (color == i) 1.0 else 0.0))
      }._2
      val rng = new Random(seed)
      val new_color = if (active) sampleColor(new_dist, rng.nextDouble) else color
      (new_color, new_dist, active, rng.nextLong)
    }
    Pregel(distGraph, true, activeDirection = EdgeDirection.Either)(vprog, sendMessage, _ || _).mapVertices((_, attr) => attr._1)
  }
}

object CFLVertexColoringExample {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Vertex Coloring Example"))

    // construct a complete graph with numVertices vertices
    val numVertices = 9
    val vids: List[Int] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[Int]]()) {
        (list, j) => if (j != i) list :+ Edge(i, j, 0) else list
      }
    })
    // all vertices have Color = 1 at the beginning
    val graph: Graph[Int, Int] = Graph.fromEdges(edges, defaultValue = 1)

    println("Finding a vertex coloring for a " + numVertices + "-clique...")

    val resGraph = CFLVertexColoring.run(graph, 0.5, numVertices)
    resGraph.vertices.collect().foreach(v => println("Vertex(" + v._1 + ", " + v._2 + ")"))
  }
}