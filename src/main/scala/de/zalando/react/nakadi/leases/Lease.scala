package de.zalando.react.nakadi.leases

import org.joda.time.DateTime


case class Lease(partitionId: String,
                 checkpointId: String,
                 leaseHolder: Option[String],
                 leaseTimestamp: Option[DateTime],
                 leaseCounter: Long,
                 leaseId: String) {

  def isExpired(counter: Long) = leaseCounter != counter
}

case class UpdateLease(partitionId: String,
                       checkpointId: Option[String] = None,
                       leaseHolder: Option[String] = None,
                       leaseTimestamp: Option[DateTime] = None,
                       leaseCounter: Long,
                       leaseId: Option[String] = None)
