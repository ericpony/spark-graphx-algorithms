import leon.lang._

object Pregel {

  type VertexId = Int

  def PregelVerificationSkeleton[VD, ED, MD] (
    num_vertices : Int,
    edge : (VertexId, VertexId) => Boolean
  ) : Boolean = {

    def isVertex (vid : VertexId) : Boolean = {
      1 <= vid && vid <= num_vertices
    }

    // Indicate whether src and dst are adjacent
    def isAdjacent (src : VertexId, dst : VertexId) : Boolean = {
      edge(src, dst) || edge(src, dst)
    }

    // Get the attribute of vertex vid in the nth round
    def vAttr (n : Int, vid : VertexId) : VD = {
      require(isVertex(vid))
      ???
    }

    // Get the attribute of the given edge in the nth round
    def eAttr (n : Int, src : VertexId, dst : VertexId) : ED = {
      require(edge(src, dst))
      ???
    }

    def message[MD] (n : Int, src : VertexId, dst : VertexId) : MD = {
      require(edge(src, dst))
      ???
    } //ensuring (activate(n, src, dst, _))

    // Indicate whether a message is sent from src to dst in the nth round
    def activate[MD] (n : Int, src : VertexId, dst : VertexId, msg : MD) : Boolean = {
      require(edge(src, dst))
      exists((src : VertexId, dst : VertexId) => message(n, src, dst) == msg)
    }

    def sendMessage (n : Int, src : VertexId, dst : VertexId) : List[(VertexId, MD)] = {
      require(edge(src, dst))
      List()
    } ensuring { list : List[(Int, MD)] =>
      forall((vid : VertexId) => (list.contains(vid) ==> (vid == src || vid == dst))) &&
        forall((vid : VertexId, msg : MD) => (list.contains(vid) ==>
          (if (src == vid) {
            message(n + 1, dst, src) == msg && activate(n + 1, dst, src, msg)
          } else {
            message(n + 1, src, dst) == msg && activate(n + 1, src, dst, msg)
          })))
    }

    // Define a total ordering on messages.
    // Needed to assert merge functions such as min and max.
    def ordering[MD] (m1 : MD, m2 : MD) : Boolean = ???

    // Reasonable candidates of mergeMsg are: min, max, choose, and sum.
    def mergeMsg_minmax[MD] (m1 : MD, m2 : MD) : MD = {
      if (ordering(m1, m2)) m1 else m2
    } ensuring (res => ordering(res, m1) && ordering(res, m2))

    def mergeMsg_choose[MD] (m1 : MD, m2 : MD) : MD = {
      m1 // or m2
    } ensuring (res => res == m1 || res == m2)

    def mergeMsg_sum (m1 : Int, m2 : Int) : Int = {
      m1 + m2
    }

    def mergeMsg[MD] (m1 : MD, m2 : MD) : MD = {
      ???
    }

    def vprog[MD] (n : Int, vid : VertexId, state : VD, msg : MD) : VD = {
      require(exists((other : VertexId) => activate(n, other, vid, msg)))

      // This pre-condition is for merge functions such as sum
      require(isAssociative(mergeMsg) && isCommutative(mergeMsg) &&
        exists((acc : VertexId => Int) =>
          msg == acc(num_vertices) &&
            forall((k : VertexId) =>
              ((k <= 0 || k > num_vertices) ==> (acc(k) == 0)) &&
                ((k > 0 && k <= num_vertices) ==> (acc(k) == mergeMsg(acc(k - 1), message(n, k, vid))))
            )))
      // This pre-condition is a special case of the above condition
      // when the merge function is min, max, etc.
      require(forall((other : VertexId, other_msg : MD) =>
        activate(n, other, vid, other_msg) ==> ordering(msg, other_msg)))

      state
    } ensuring { res =>
      vAttr(n + 1, vid) == res
    }

    // TODO: Define loop invariant for Pregel loop
    true
  }

  def isAssociative[A] (f : (A, A) => A) : Boolean = {
    forall((x : A, y : A, z : A) => f(f(x, y), z) == f(x, f(y, z)))
  }

  def isCommutative[A] (f : (A, A) => A) : Boolean = {
    forall((x : A, y : A) => f(x, y) == f(y, x))
  }

  def exists[A] (p : A => Boolean) = !forall[A](!p(_))
  def exists[A, B] (p : (A, B) => Boolean) = !forall[A, B]((a, b) => !p(a, b))
}

