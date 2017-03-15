package de.zalando.react.nakadi.leases.impl

import akka.actor.{Actor, ActorLogging, ActorRef}
import de.zalando.react.nakadi.leases.{Lease, LeaseCoordinator}

import scala.concurrent.Future


class LeaseCoordinatorActor extends Actor with ActorLogging with LeaseCoordinator {

  override val allLeases: Map[String, Lease] = Map.empty

  override val workerIdToWorker: Map[String, ActorRef] = Map.empty

  override def receive: Receive = ???

  override def pingWorker: Future[Boolean] = ???

  override def takeLease: Future[Boolean] = ???
}
