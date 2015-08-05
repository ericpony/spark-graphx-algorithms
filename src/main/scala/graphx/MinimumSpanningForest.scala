package graphx

import org.apache.spark.graphx._

object Types  {
  type EdgeWeight = Double
}
object MinimumSpanningForest extends Logger {

  import scala.reflect.ClassTag
  import Types._

  def run[VD: ClassTag] (graph: Graph[VD, EdgeWeight], numIter: Int = Int.MaxValue): Graph[VertexId, EdgeWeight] = {

      // each vertex attribute is the id of the vertex at the minimal outgoing edge
      val vRDD = graph.aggregateMessages[(VertexId, EdgeWeight)](
      ctx => ctx.sendToSrc((ctx.dstId, ctx.attr)), {
        case ((v1, w1), (v2, w2)) =>
          if (w1 > w2) (v2, w2) else (v1, w1)
      })

      info("=== before identifying super-vertices ===")
      vRDD.collect().foreach(v => info("Vertex(" + v._1 + ", (" + v._2._1 + ", " + v._2._2 + "))"))
      info("=====")

      // each vertex attribute containts the vertex id of its parent node in the MSF
      val superVertexGraph = Graph(vRDD.mapValues[VertexId]((v: (VertexId, EdgeWeight)) => v._1 ), graph.edges)

      info("=== after identifying super-vertices ===")
      superVertexGraph.vertices.collect().foreach(v => info("Vertex(" + v._1 + ", " + v._2 + ")"))
      info("=====")

      // identify the super vertex in each tree
      Graph(superVertexGraph.aggregateMessages[VertexId](
        ctx => {
          if (ctx.dstId == ctx.srcAttr && ctx.dstAttr == ctx.srcId // detect a 2-cycle
              && ctx.dstId < ctx.srcId)    // dst is a super-vertex
            ctx.sendToDst(ctx.dstId)       // let dst point to itself
          else ctx.sendToDst(ctx.dstAttr)  // let dst keep the current pointer
        }, (vid1, vid2) => math.min(vid1, vid2)
      ), graph.edges)
  }
}
object MinimumSpanningForestExample {

  import org.apache.spark.{SparkConf, SparkContext}
  import scala.util.Random
  import Types._

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("MSF Example"))
    val rng = new Random(12345)
    // construct a complete graph with numVertices vertices
    val numVertices = 10
    val vids: List[Int] = (1 to numVertices).toList
    val edges = sc.parallelize(vids.flatMap {
      i => vids.foldLeft(List[Edge[EdgeWeight]]()) {
        (list, j) => {
          val w = rng.nextInt(10) * 1.0
          if (j > i) list :+ Edge(i, j, w) :+ Edge(j, i, w)
          else list
        }
      }
    })
    println("Edges: ")
    edges.cache().collect().filter(e => e.srcId < e.dstId).foreach(e => println(e.srcId + " <-- " + e.attr + " --> " + e.dstId))

    // all vertices have Color = 1 at the beginning
    val graph: Graph[Int, EdgeWeight] = Graph.fromEdges(edges, 0)

    println("Finding a vertex coloring for a " + numVertices + "-clique...")

    val resGraph = MinimumSpanningForest.run(graph)
    resGraph.vertices.collect().foreach(v => println("Vertex(" + v._1 + ", " + v._2 + ")"))
  }
}