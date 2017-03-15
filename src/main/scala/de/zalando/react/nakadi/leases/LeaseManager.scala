package de.zalando.react.nakadi.leases

import scala.concurrent.Future


trait LeaseManager {

  def getLease(groupId: String, topic: String, partitionId: String): Future[Option[Lease]]

  def updateLease(groupId: String, topic: String, lease: UpdateLease): Future[Boolean]

  def createLeaseIfNotExists(groupId: String, topic: String, lease: Lease): Future[Boolean]

  def listAll(groupId: String, topic: String, limit: Option[Int] = None): Future[Seq[Lease]]

  def deleteLease(groupId: String, topic: String, partitionId: String): Future[Boolean]

  def deleteAll(groupId: String, topic: String): Future[Boolean]

  def renewLease(groupId: String, topic: String, lease: Lease): Future[Option[Lease]]

  def takeLease(groupId: String, topic: String, lease: Lease): Future[Option[Lease]]

  def commitLease(groupId: String, topic: String, lease: Lease): Future[Option[Lease]]

  def evictLease(groupId: String, topic: String, lease: Lease): Future[Option[Lease]]

}
