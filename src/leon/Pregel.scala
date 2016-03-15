import leon.lang._
import leon.lang.synthesis._

type VertexId = Int

class PregelVerificationSkeleton[VD, ED, Message] (
  num_vertices : Int,
  vAttr : VertexId => VD,
  eAttr : (VertexId, VertexId) => ED,
  edge : (VertexId, VertexId) => Boolean,
  initial_message : Message) {

  /* Graph properties */
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
    if (n == 0) vAttr(vid)
    else choose((attr : VD) => true)
  }

  // Get the attribute of the given edge in the nth round
  def eAttr (n : Int, src : VertexId, dst : VertexId) : ED = {
    require(edge(src, dst))
    if (n == 0) eAttr(src, dst)
    else choose((attr : ED) => true)
  }
  /* End of graph properties */

  /* Helper functions */
  // Indicate whether a message is sent from src to dst in the nth round
  def activate[Message] (n : Int, src : VertexId, dst : VertexId) : Boolean = {
    require(edge(src, dst))
    choose((active : Boolean) => true)
  }

  def isActive (n : Int, vid : VertexId) : Boolean = {
    exists((other : VertexId) => activate(n, other, vid))
  }

  def message[Message] (n : Int, src : VertexId, dst : VertexId) : Message = {
    require(edge(src, dst) && activate(n, src, dst))
    choose((msg : Message) => true)
  }
  /* End of helper functions */

  /* User-defined functions for Pregel */
  def sendMessage (n : Int, src : VertexId, dst : VertexId) : List[(VertexId, Message)] = {
    require(edge(src, dst))
    ???
  } ensuring { list : List[(Int, Message)] =>
    forall((vid : VertexId, msg : Message) =>
      (list.contains((vid, msg), msg) ==> (vid == src || vid == dst))
    ) && forall((msg : Message) =>
      (list.contains((src, msg)) ==>
        (message(n + 1, dst, src) == msg && activate(n + 1, dst, src))) &&
        (list.contains((dst, msg)) ==>
          (message(n + 1, src, dst) == msg && activate(n + 1, src, dst))) &&
        (forall((msg : Message) => !list.contains((src, msg)))) ==> !activate(n + 1, dst, src) &&
        (forall((msg : Message) => !list.contains((dst, msg)))) ==> !activate(n + 1, src, dst))
  }

  def mergeMsg[Message] (m1 : Message, m2 : Message) : Message = {
    ???
  }

  def vprog[Message] (n : Int, vid : VertexId, state : VD, msg : Message) : VD = {
    require(isActive(n, vid))
    require(/* isAssociative(mergeMsg) && isCommutative(mergeMsg) && */
      exists((acc : VertexId => Int) =>
        msg == acc(num_vertices) &&
          forall((k : VertexId) =>
            ((k <= 0) ==> (acc(k) == 0)) &&
              ((k > 0 && k <= num_vertices) ==> (acc(k) == (
                if (!activate(n, k, vid)) acc(k - 1)
                else mergeMsg(acc(k - 1), message(n, k, vid)))))
          )))
    ???
  } ensuring { res => vAttr(n + 1, vid) == res }
  /* End of user-defined functions */

  def PregelLoop = {
    require(forall((vid : VertexId) => activate(0, vid, vid)))
    var num_iterations = 0
    while (exists((vid : VertexId) => isActive(num_iterations, vid))) {
      1 to num_vertices foreach (dst => {
        if (num_iterations == 0) {
          vprog(0, dst, vAttr(dst), initial_message)
        } else {
          val messages = 1 to num_vertices filter (activate(num_iterations, _, dst)) map (message(num_iterations, _, dst))
          if (messages.nonEmpty) {
            vprog(num_iterations, dst, vAttr(num_iterations, dst), messages.reduce(mergeMsg))
          }
        }
      })
      0 to num_vertices * num_vertices - 1 foreach (vid =>
        sendMessage(num_iterations, vid % num_vertices + 1, vid / num_vertices + 1))
      num_iterations = num_iterations + 1
    }
  }

  def isAssociative[A] (f : (A, A) => A) : Boolean = {
    forall((x : A, y : A, z : A) => f(f(x, y), z) == f(x, f(y, z)))
  }

  def isCommutative[A] (f : (A, A) => A) : Boolean = {
    forall((x : A, y : A) => f(x, y) == f(y, x))
  }

  def exists[A] (p : A => Boolean) = !forall[A](!p(_))
  def exists[A, B] (p : (A, B) => Boolean) = !forall[A, B]((a, b) => !p(a, b))
  def exists[A, B, C] (p : (A, B, C) => Boolean) = !forall[A, B, C]((a, b, c) => !p(a, b, c))
}

