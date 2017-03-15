package de.zalando.react.nakadi.leases.impl

import akka.actor.{Actor, ActorLogging}
import de.zalando.react.nakadi.leases.{Lease, LeaseManager, LeaseWorker}

import scala.concurrent.Future


class LeaseWorkerActor(override val topic: String,
                       override val groupId: String,
                       override val partitionId: String,
                       override val leaseManager: LeaseManager) extends Actor with ActorLogging with LeaseWorker {

  override def receive: Receive = ???

  override def renew(lease: Lease): Future[Boolean] = ???

  override def commit(lease: Lease): Future[Boolean] = ???

  override def die: Future[Boolean] = ???
}
