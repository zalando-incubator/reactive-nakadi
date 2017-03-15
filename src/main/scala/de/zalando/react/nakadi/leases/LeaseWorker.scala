package de.zalando.react.nakadi.leases

import scala.concurrent.Future


trait LeaseWorker {

  def topic: String

  def groupId: String

  def partitionId: String

  def leaseManager: LeaseManager

  def renew(lease: Lease): Future[Boolean]

  def die: Future[Boolean]

  def commit(lease: Lease): Future[Boolean]

}
