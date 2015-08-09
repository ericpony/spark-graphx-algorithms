package graphx

import org.apache.spark.graphx._

import scala.reflect.ClassTag
import scala.util.Random

/**
 * A pregel implementation of a randomized maximal independent set algorithm.
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

  /* Note that edges in the input graph are treated as undirected */
  def apply[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED],
                                         maxNumIterations: Int = Int.MaxValue,
                                         isConnected: Boolean = false): Graph[Boolean, ED] = {
    val Unknown = 0
    val Selected = 1
    val Excluded = 2
    val seed = Random.nextLong
    val space = Random.nextInt
    var numIterations = 0L

    // all vertices are in the candidate MIS at the beginning
    var misGraph = graph.mapVertices((_, _) => true)

    // compute working graph, which has vertex attributes as (status, probability, RNG)
    var misWorkGraph = {
      // remove the isolated vertices from working graph
      val g = graph.outerJoinVertices(graph.degrees) {
        (vid, data, deg) => deg.getOrElse(0)
      }
      if (isConnected) g else g.subgraph(vpred = (_, deg) => deg > 0)
    }.mapVertices {
      (vid, deg) => (Unknown, 1.0 / (2 * deg), new Random(seed + vid * space))
    }.cache()

    // Counting edges instead of vertices help us avoid
    // some unnecessary iterations in the main loop.
    var numEdges = misWorkGraph.numEdges

    while (numEdges > 0 && numIterations < maxNumIterations) {
      numIterations = numIterations + 1

      // mark each Unknown vertex as Selected randomly
      misWorkGraph = misWorkGraph.mapVertices {
        (v, data) =>
          // make decision by tossing a coin of head probability 1/(2*deg(v))
          val nextState = if (data._3.nextDouble < data._2) Selected else Unknown
          (nextState, data._2, data._3)
      }.cache()

      // mark the neighbors of each Selected vertex as Unknown
      misWorkGraph = misWorkGraph.joinVertices(misWorkGraph.aggregateMessages[VertexState](
        e => {
          // note that we cannot exclude vertices at this stage,
          // for otherwise we are likely to exclude too many vertices
          if (e.srcAttr._1 == Selected && e.dstAttr._1 == Selected)
            (if (e.srcId < e.dstId) e.sendToDst _ else e.sendToSrc _)(Unknown)
        },
        (_, s) => s
      )) {
        (vid, attr, state) => (state, attr._2, attr._3)
      }

      // identify the vertices to exclude, i.e., the vertices near Selected vertices
      val exVertices = misWorkGraph.aggregateMessages[VertexState](
        e => {
          if (e.srcAttr._1 == Selected && e.dstAttr._1 == Unknown)
            e.sendToDst(Excluded)
          else if (e.srcAttr._1 == Unknown && e.dstAttr._1 == Selected)
            e.sendToSrc(Excluded)
        },
        (_, s) => s
      ).filter(_._2 == Excluded).cache()

      if (exVertices.count() > 0) {
        // remove Excluded vertices from the candidate MIS
        misGraph = misGraph.joinVertices(exVertices)((_, _, _) => false)

        // keep non-isolated Unknown vertices in working graph
        misWorkGraph = misWorkGraph.filter[Boolean, ED](
          g => {
            g.outerJoinVertices(g.aggregateMessages[Boolean](
              e => {
                // remove Selected vertices and their neighbors from working graph
                val remove = e.srcAttr._1 == Selected || e.dstAttr._1 == Selected
                e.sendToDst(remove)
                e.sendToSrc(remove)
              }, _ || _
            )) {
              // remove the vertices that didn't receive any messages
              (_, _, b) => b.getOrElse(true)
            }
          },
          vpred = (_, b) => !b
        ).cache()

        // update probabilities according to new degrees
        misWorkGraph = misWorkGraph.joinVertices(misWorkGraph.degrees) {
          (vid, attr, deg) => (Unknown, 1.0 / (2 * deg), attr._3)
        }.cache()

        // stop if there are no adjacent Unknown vertices in working graph
        numEdges = misWorkGraph.numEdges
      }
    }
    // Eventually, all vertices near Selected vertices are Excluded from working graph.
    // Since we put all vertices in MIS at the beginning, the final MIS includes exactly
    // the Selected vertices and the Unknown vertices left after the loop terminated.
    misGraph
  }

  /* Find invalid vertices in the provided MIS graph and return descriptive error messages */
  def findInvalidVertices (graph: Graph[Boolean, _], maxNumMessages: Int = 1): Array[(VertexId, String)] = {
    // for each vertex, count the number of Selected neighbors
    graph.outerJoinVertices(graph.aggregateMessages[Long](
      ctx => {
        ctx.sendToDst(if (ctx.srcAttr) 1 else 0)
        ctx.sendToSrc(if (ctx.dstAttr) 1 else 0)
      },
      _ + _
    )) {
      (vid, selected, opt) => {
        if (!selected && !opt.isDefined) "isolated vertex not selected"
        else if (!selected && opt.get == 0) "not maximal"
        else if (selected && opt.getOrElse(0L) > 0) "not independent"
        else null
      }
    }.vertices.filter(v => v._2 != null).take(maxNumMessages)
  }

  /* Verify the provided MIS graph */
  def verify (graph: Graph[Boolean, _]): Boolean = findInvalidVertices(graph).length == 0
}