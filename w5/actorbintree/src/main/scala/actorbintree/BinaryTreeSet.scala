/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {

  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(sender, identifier, el) => root ! Insert(sender, identifier, el)
    case Contains(sender, identifier, el) => root ! Contains(sender, identifier, el)
    case Remove(sender, identifier, el) => root ! Remove(sender, identifier, el)
    case GC =>
      val newRoot = createRoot
      context.become(garbageCollecting(newRoot))
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    // Perform copy operation
    case op: Operation => pendingQueue = pendingQueue :+ op
    case CopyFinished =>
      root = newRoot

      context.become(normal)

      pendingQueue.foreach(root ! _)

      pendingQueue = Queue.empty[Operation]

    case GC =>
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(sender, identifier, el) =>
      if (elem == el) {
        removed = false
        sender ! OperationFinished(identifier)
      } else if (el < elem && subtrees.exists(_._1 == Left)) {
        subtrees(Left) ! Insert(sender, identifier, el)
      } else if (el > elem && subtrees.exists(_._1 == Right)) {
        subtrees(Right) ! Insert(sender, identifier, el)
      } else {
        // The element does not exist
        if (el < elem) {
          subtrees += Left -> context.actorOf(BinaryTreeNode.props(el, initiallyRemoved = false))
        } else {
          subtrees += Right -> context.actorOf(BinaryTreeNode.props(el, initiallyRemoved = false))
        }

        sender ! OperationFinished(identifier)
      }

    case Contains(sender, identifier, el) =>
      if (elem == el) {
        sender ! ContainsResult(identifier, result = !removed)
      } else if (el < elem && subtrees.exists(_._1 == Left)) {
        subtrees(Left) ! Contains(sender, identifier, el)
      } else if (el > elem && subtrees.exists(_._1 == Right)) {
        subtrees(Right) ! Contains(sender, identifier, el)
      } else {
        sender ! ContainsResult(identifier, result = false)
      }

    case Remove(sender, identifier, el) =>
      if (elem == el) {
        removed = true
        sender ! OperationFinished(identifier)
      } else if (el < elem && subtrees.exists(_._1 == Left)) {
        subtrees(Left) ! Remove(sender, identifier, el)
      } else if (el > elem && subtrees.exists(_._1 == Right)) {
        subtrees(Right) ! Remove(sender, identifier, el)
      } else {
        sender ! OperationFinished(identifier)
      }

    case CopyTo(newTree) =>
      val children = subtrees.values.toSet

      children.foreach(_ ! CopyTo(newTree))

      if (!removed) {
        newTree ! Insert(self, -1, elem)
      }

      if (children.isEmpty && removed) {
        self ! PoisonPill
      } else {
        context.become(copying(children, removed))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(-1) =>
      if (expected.isEmpty) {
        self ! PoisonPill
      } else {
        context.become(copying(expected, insertConfirmed = true))
      }

    case CopyFinished =>
      val current = expected - context.sender()

      if (current.isEmpty && insertConfirmed) {
        self ! PoisonPill
      } else {
        context.become(copying(current, insertConfirmed))
      }
  }

  override def postStop(): Unit = context.parent ! CopyFinished
}
