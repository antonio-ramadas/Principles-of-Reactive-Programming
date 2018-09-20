package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import kvstore.Arbiter._

import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart

import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import akka.actor.Terminated

import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  override def preStart(): Unit = {
    super.preStart()
    arbiter ! Join
  }

  val persistence: ActorRef = context.actorOf(persistenceProps)

  var _seqCounter = 0L

  def receive: PartialFunction[Any, Unit] = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv = kv + (key -> value)

      PersistenceHandler.addMessage(id, 100.millisecond, sender, isPersisted = false)

      PersistenceHandler.primaryRetryPersist(key, Some(value), id, 100.millisecond)

      replicators.foreach(actor => actor ! Replicate(key, Some(value), id))

      context.system.scheduler.scheduleOnce(1.second, self, ReplicationFail(id))

    case Remove(key, id) =>
      kv = kv - key

      PersistenceHandler.addMessage(id, 100.millisecond, sender, isPersisted = false)

      PersistenceHandler.primaryRetryPersist(key, None, id, 100.millisecond)

      replicators.foreach(actor => actor ! Replicate(key, None, id))

      context.system.scheduler.scheduleOnce(1.second, self, ReplicationFail(id))

    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case Persisted(key, seq) =>
      if (PersistenceHandler.containsMessage(seq)) {

        PersistenceHandler.setPersisted(seq, state = true)

        if (PersistenceHandler.hasEveryoneAcks(seq, replicators)) {
          PersistenceHandler.getActorRef(seq) ! OperationAck(seq)
          PersistenceHandler.removeMessage(seq)
        }
      }

    case Replicas(replicas) =>
      val secondaryReplicas = secondaries.keys.toSet

      val actorsWithoutSelf = replicas - self

      val newReplicas = actorsWithoutSelf.diff(secondaryReplicas)

      val oldReplicas = secondaryReplicas.diff(actorsWithoutSelf)

      oldReplicas.foreach(replica => {
        secondaries(replica) ! PoisonPill
      })

      newReplicas.foreach(replica => {
        val replicator = context.actorOf(Replicator.props(replica))

        secondaries = secondaries.updated(replica, replicator)

        kv.foreach({ case (k,v) => replicator ! Replicate(k, Some(v), secondaries.size)})
      })

      replicators = secondaries.values.toSet


    case Replicated(key, seq) =>
      if (PersistenceHandler.containsMessage(seq)) {

        PersistenceHandler.addSnapshotAcks(seq, sender)

        if (PersistenceHandler.hasEveryoneAcks(seq, replicators) && PersistenceHandler.isPersisted(seq)) {
          //PersistenceHandler.getActorRef(seq) ! OperationAck(seq)
          //PersistenceHandler.removeMessage(seq)
        }
      }

    case ReplicationFail(seq) =>
      if (PersistenceHandler.containsMessage(seq)) {
        if (replicators.diff(PersistenceHandler.getSnapshotAcks(seq)).isEmpty && PersistenceHandler.isPersisted(seq)) {
          PersistenceHandler.getActorRef(seq) ! OperationAck(seq)
        } else {
          PersistenceHandler.getActorRef(seq) ! OperationFailed(seq)
        }
        PersistenceHandler.removeMessage(seq)
      }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOption, seq) =>
      if (seq > _seqCounter) {}
      else if (seq < _seqCounter) {
        sender ! SnapshotAck(key, seq)
      } else {
        PersistenceHandler.addMessage(seq, actorRef = sender)

        PersistenceHandler.SecondaryRetryPersist(key, valueOption, seq)

        valueOption match {
          case Some(x) => kv = kv + (key -> x)
          case None => kv = kv - key
        }
        _seqCounter += 1
      }

    case Persisted(key, seq) =>
      PersistenceHandler.getActorRef(seq) ! SnapshotAck(key, seq)
      PersistenceHandler.removeMessage(seq)
  }

  private object PersistenceHandler {

    private case class MessageData(currTime: FiniteDuration = 0.second, acks: Set[ActorRef] = Set.empty, actorRef: ActorRef, isPersisted: Boolean = true)

    private var msgs = Map.empty[Long, MessageData]

    def addMessage(seq: Long, currTime: FiniteDuration = 0.second, actorRef: ActorRef, isPersisted: Boolean = true): Unit = {
      msgs += (seq -> MessageData(currTime, actorRef = actorRef, isPersisted = isPersisted))
    }

    def SecondaryRetryPersist(key: String, valueOption: Option[String], seq: Long): Unit = {
      persistence ! Persist(key, valueOption, seq)

      context.system.scheduler.scheduleOnce(100.milliseconds) {
        if (containsMessage(seq))
          SecondaryRetryPersist(key, valueOption, seq)
      }
    }

    def primaryRetryPersist(key: String, value: Option[String], id: Long, delay: FiniteDuration): Unit = {
      persistence ! Persist(key, value, id)

      context.system.scheduler.scheduleOnce(delay) {
        if (containsMessage(id))
          if (getCurrTime(id) < 1.second) {
            primaryRetryPersist(key, value, id, delay)
            incCurrTime(id, delay)
          } else {
            getActorRef(id) ! OperationFailed(id)
            removeMessage(id)
          }
      }
    }

    def getSnapshotAcks(seq: Long): Set[ActorRef] = msgs(seq).acks

    def getActorRef(seq: Long): ActorRef = msgs(seq).actorRef

    def removeMessage(seq: Long): Unit = msgs = msgs - seq

    def getCurrTime(seq: Long): FiniteDuration = msgs(seq).currTime

    def containsMessage(seq: Long): Boolean = msgs.contains(seq)

    def isPersisted(seq: Long): Boolean = msgs(seq).isPersisted

    def setPersisted(seq: Long, state: Boolean): Unit = {
      val msg = msgs(seq)

      val newMsg = MessageData(msg.currTime, msg.acks, msg.actorRef, state)

      msgs = msgs.updated(seq, newMsg)
    }

    def hasEveryoneAcks(seq: Long, nodes: Set[ActorRef]): Boolean = nodes.diff(msgs(seq).acks).isEmpty

    def addSnapshotAcks(seq: Long, actor: ActorRef): Unit = {
      val msg = msgs(seq)

      val newMsg = MessageData(msg.currTime, msg.acks + actor, msg.actorRef, msg.isPersisted)

      msgs = msgs.updated(seq, newMsg)
    }

    def incCurrTime(seq: Long, delay: FiniteDuration): Unit = {
      val msg = msgs(seq)

      val newMsg = MessageData(msg.currTime + delay, msg.acks, msg.actorRef, msg.isPersisted)

      msgs = msgs.updated(seq, newMsg)
    }
  }

  case class ReplicationFail(l: Long)

}

