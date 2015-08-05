package graphx

import scala.util.Random

object CFLVertexColoring {

  import graphx.Types._
  import org.apache.spark.graphx._

  type Palette = (Color, List[Double], Boolean, Random)

  private def sampleColor (dist: List[Double], rnd: Double): Int = {
    dist.foldLeft((1, 0.0)) {
      case ((color, mass), weight) => {
        val m = mass + weight
        (if (m < rnd) color + 1 else color, m)
      }
    }._1
  }

  def run (graph: Graph[Color, _], beta: Double, maxNumColors: Int): Graph[Color, _] = {

    val seed = Random.nextLong
    val space = Random.nextInt
    val initColorDist = (1 to maxNumColors).toList.map(_ => 1.0 / maxNumColors)
    val distGraph = graph.mapVertices((id, attr) => (attr, initColorDist, true, new Random(seed + id * space)))

    def sendMessage (edge: EdgeTriplet[Palette, _]): Iterator[(VertexId, Boolean)] = {
      if (edge.srcAttr._1 == edge.dstAttr._1)
        return Iterator((edge.srcId, true))
      if (edge.srcAttr._3)
        return Iterator((edge.srcId, false))
      Iterator.empty
    }
    def vprog (id: VertexId, attr: Palette, active: Boolean): Palette = {
      val color = attr._1
      val dist = attr._2
      val rng = attr._4
      val new_dist = dist.foldLeft((1, List[Double]())) {
        case ((i, list), weight) => (i + 1,
          if (active)
            list :+ (weight * (1 - beta) + (if (color == i) 0.0 else beta / (maxNumColors - 1)))
          else
            list :+ (if (color == i) 1.0 else 0.0))
      }._2
      val new_color = if (active) sampleColor(new_dist, rng.nextDouble) else color
      (new_color, new_dist, active, rng)
    }
    Pregel(distGraph, true, activeDirection = EdgeDirection.Either)(vprog, sendMessage, _ || _).mapVertices((_, attr) => attr._1)
  }
}

object CFLVertexColoringExample {

  import graphx.Types._
  import org.apache.spark.graphx._
  import org.apache.spark.{SparkConf, SparkContext}

  def main (args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Vertex Coloring Example"))

    // construct a complete graph with numVertices vertices
    val numVertices = 9
    val vids: List[Color] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[Null]]()) {
        (list, j) => if (j != i) list :+ Edge(i, j, null) else list
      }
    })
    // all vertices have Color = 1 at the beginning
    val graph: Graph[Color, Null] = Graph.fromEdges(edges, defaultValue = 1)

    println("Finding a vertex coloring for a " + numVertices + "-clique...")

    val resGraph = CFLVertexColoring.run(graph, 0.5, numVertices)
    resGraph.vertices.collect().foreach(v => println("Vertex(" + v._1 + ", " + v._2 + ")"))
  }
}