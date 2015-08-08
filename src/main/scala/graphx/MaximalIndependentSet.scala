package graphx

import org.apache.spark.graphx._

import scala.reflect.ClassTag
import scala.util.Random

/**
 * A pregel implementation of a randomized greedy maximal independent set algorithm.
 *
 * Author:
 * ericpony (https://github.com/ericpony/graphx-examples)
 *
 * Reference:
 * M. Luby. A Simple Parallel Algorithm for the Maximal Independent Set Problem.
 * SIAM Journal on Computing, 15(4), 1986.
 */
object MaximalIndependentSet {
  type VertexState = Int

  /**
   * Remark: the input graph will be treated as an undirected graph.
   */
  def apply[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED],
                                         maxNumIterations: Int = Int.MaxValue,
                                         isConnected: Boolean = false): Graph[Boolean, ED] = {
    val rng = new Random(54321) // for debug purpose
    val Unknown = 0
    val Selected = 1
    val Excluded = 2
    val seed = rng.nextLong
    val space = rng.nextInt
    var numIterations = 0L

    // select all vertices at the beginning
    var misGraph = graph.mapVertices((_, _) => true)

    // compute working graph with vertex attribute: (status, probability, RNG)
    var misWorkGraph = {
      // remove isolated vertices from the working graph
      val g = graph.outerJoinVertices(graph.degrees) {
        (vid, data, deg) => deg.getOrElse(0)
      }
      if (isConnected) g else g.subgraph(vpred = (_, deg) => deg > 0)
    }.mapVertices {
      (vid, deg) => (Unknown, 1.0 / (2 * deg), new Random(seed + vid * space))
    }.cache()

    var numEdges = misWorkGraph.numEdges

    while (numEdges > 0 && numIterations < maxNumIterations) {
      numIterations = numIterations + 1

      // select each Unknown vertex randomly
      misWorkGraph = misWorkGraph.mapVertices {
        (v, data) =>
          // make decision according to current probability
          val nextState = if (data._3.nextDouble < data._2) Selected else Unknown
          (nextState, data._2, data._3)
      }.cache()

      // set the neighbors of the Selected vertices to Unknown
      misWorkGraph = misWorkGraph.joinVertices(misWorkGraph.aggregateMessages[VertexState](
        e => {
          if (e.srcAttr._1 == Selected && e.dstAttr._1 == Selected)
            (if (e.srcId < e.dstId) e.sendToDst _ else e.sendToSrc _)(Unknown)
        },
        (_, s) => s
      )) {
        (vid, attr, state) => (state, attr._2, attr._3)
      }

      // identify the vertices to exclude, i.e., vertices near Selected vertices
      val exVertices = misWorkGraph.aggregateMessages[VertexState](
        e => {
          if (e.srcAttr._1 == Selected && e.dstAttr._1 == Unknown)
            e.sendToDst(Excluded)
          else if (e.srcAttr._1 == Unknown && e.dstAttr._1 == Selected)
            e.sendToSrc(Excluded)
        },
        math.max(_, _)
      ).filter(_._2 == Excluded).cache()

      if (exVertices.count() > 0) {
        // update base graph according to the excluded vertices
        misGraph = misGraph.joinVertices(exVertices)((_, _, _) => false)

        // keep non-isolated Unknown vertices in working graph
        misWorkGraph = misWorkGraph.filter[Boolean, ED](
          g => {
            g.outerJoinVertices(g.aggregateMessages[Boolean](
              e => {
                // decide whether to remove a vertex or not
                val remove = e.srcAttr._1 == Selected || e.dstAttr._1 == Selected
                e.sendToDst(remove)
                e.sendToSrc(remove)
              }, _ || _
            )) {
              (_, _, b) => b.getOrElse(true)
            }
          },
          vpred = (_, b) => !b
        ).cache()

        // update probabilities according to new degrees
        misWorkGraph = misWorkGraph.joinVertices(misWorkGraph.degrees) {
          (vid, attr, deg) => (Unknown, 1.0 / (2 * deg), attr._3)
        }.cache()

        // stop if no Unknown vertices are left
        numEdges = misWorkGraph.numEdges
      }
    }
    // eventually, all vertices in the working graph are excluded
    misGraph
  }

  def findInvalidVertices (graph: Graph[Boolean, _], maxNumMessages: Int = 1): Array[(VertexId, String)] = {
    // for each vertex, count the number of selected neighbors
    graph.outerJoinVertices(graph.aggregateMessages[Long](
      ctx => {
        ctx.sendToDst(if (ctx.srcAttr) 1 else 0)
        ctx.sendToSrc(if (ctx.dstAttr) 1 else 0)
      },
      _ + _
    )) {
      (vid, selected, opt) => {
        if (!selected && !opt.isDefined) "isolated vertex not selected"
        else if (selected && opt.getOrElse(0L) > 0) "not independent"
        else if (!selected && opt.get == 0) "not maximal"
        else null
      }
    }.vertices.filter(v => v._2 != null).take(maxNumMessages)
  }

  def verify (graph: Graph[Boolean, _]): Boolean = findInvalidVertices(graph).length == 0
}