package de.zalando.react.nakadi.leases.impl

import java.util

import akka.actor.ActorSystem
import org.joda.time.{DateTime, DateTimeZone}
import com.amazonaws.services.dynamodbv2.model._
import de.zalando.react.nakadi.leases.{Lease, LeaseManager, UpdateLease}

import scala.concurrent.Future
import collection.JavaConverters._
import scala.util.{Failure, Success, Try}


class DynamoDBLeaseManager(system: ActorSystem, awsProvider: AWSProvider) extends LeaseManager {
  import system.dispatcher

  val log = system.log

  val PartitionIdKey = "partitionId"
  val CheckpointIdKey = "checkpointId"
  val LeaseHolderKey = "leaseHolder"
  val LeaseCounterKey = "leaseCounter"
  val LeaseTimestampKey = "leaseTimestamp"
  val LeaseIdKey = "leaseId"

  val client = awsProvider.client

  private def tableName(groupId: String, topic: String) = s"reactive-nakadi-$topic-$groupId"

  private def attributeValue(value: String) = {
    new AttributeValue().withS(value)
  }

  private def attributeValue(value: Long) = {
    new AttributeValue().withN(value.toString)
  }

  private def attributeValueUpdate(value: String) = {
    new AttributeValueUpdate(attributeValue(value), AttributeAction.PUT)
  }

  private def attributeValueUpdate(value: Long) = {
    new AttributeValueUpdate(attributeValue(value), AttributeAction.PUT)
  }

  private def attributeValueDelete = {
    new AttributeValueUpdate(null, AttributeAction.DELETE)
  }

  private def expectedCounterMatch(leaseCounter: Long): util.HashMap[String, ExpectedAttributeValue] = {
    val k = new java.util.HashMap[String, ExpectedAttributeValue]
    k.put(LeaseCounterKey, new ExpectedAttributeValue(attributeValue(leaseCounter)))
    k
  }

  private def expectedNonExist(): util.HashMap[String, ExpectedAttributeValue] = {
    val k = new java.util.HashMap[String, ExpectedAttributeValue]
    k.put(PartitionIdKey, new ExpectedAttributeValue(false))
    k
  }

  private def toLease(item: java.util.Map[String, AttributeValue]): Lease = {
    Lease(
      partitionId = Option(item.get(PartitionIdKey).getS)
        .getOrElse(sys.error("Read back empty partitionId from DynamoDB")),
      checkpointId = Option(item.get(CheckpointIdKey).getS)
        .getOrElse(sys.error("Read back empty checkpointId from DynamoDB")),
      leaseHolder = Option(item.get(LeaseHolderKey).getS),
      leaseCounter = Option(item.get(LeaseCounterKey).getN.toLong)
        .getOrElse(sys.error("Read back empty leaseCounter from DynamoDB")),
      leaseTimestamp = Option(new DateTime(item.get(LeaseTimestampKey).getS, DateTimeZone.UTC)),
      leaseId = Option(item.get(LeaseIdKey).getS)
        .getOrElse(sys.error("Read back empty leaseId from DynamoDB"))
    )
  }

  private def fromLease(lease: Lease): java.util.Map[String, AttributeValue] = {
    val k = new java.util.HashMap[String, AttributeValue]
    k.put(CheckpointIdKey, attributeValue(lease.checkpointId))
    lease.leaseHolder.map(v => k.put(LeaseHolderKey, attributeValue(v)))
    lease.leaseTimestamp.map(v => k.put(LeaseTimestampKey, attributeValue(v.toString)))
    k.put(LeaseCounterKey, attributeValue(lease.leaseCounter))
    k.put(LeaseIdKey, attributeValue(lease.leaseId))
    k
  }

  override def getLease(groupId: String, topic: String, partitionId: String): Future[Option[Lease]] = Future {
    val key = new java.util.HashMap[String, AttributeValue]
    key.put(PartitionIdKey, attributeValue(partitionId))

    val request = new GetItemRequest()
    request.setTableName(tableName(groupId, topic))
    request.setKey(key)
    request.setConsistentRead(awsProvider.consistentReads)

    Try(Option(client.getItem(request).getItem).map(toLease)) match {
      case Failure(err) =>
        log.error(err, s"AWS Error getting lease for group '$groupId' and topic '$topic' and partition $partitionId:")
        sys.error(s"AWS Error getting lease for group '$groupId' and topic '$topic' and partition $partitionId")
      case Success(lease) => lease
    }
  }

  override def deleteAll(groupId: String, topic: String): Future[Boolean] = {
    log.warning(s"Deleting all leases for group '$groupId' and topic '$topic'")

    listAll(groupId, topic).map(_.map { lease =>
      deleteLease(groupId, topic, lease.partitionId)
    }).map(_ => true)
  }

  override def deleteLease(groupId: String, topic: String, partitionId: String): Future[Boolean] = Future {
    val key = new java.util.HashMap[String, AttributeValue]
    key.put(PartitionIdKey, attributeValue(partitionId))

    val deleteRequest = new DeleteItemRequest()
    deleteRequest.setTableName(tableName(groupId, topic))
    deleteRequest.setKey(key)

    Try(client.deleteItem(deleteRequest)) match {
      case Failure(err) =>
        log.error(err, s"AWS Error deleting lease for group '$groupId' and topic '$topic':")
        sys.error(s"AWS Error deleting lease for group '$groupId' and topic '$topic'")
      case Success(_) => true
    }
  }

  override def listAll(groupId: String, topic: String, limit: Option[Int] = None): Future[Seq[Lease]] = Future {
    val scanRequest = new ScanRequest()
    scanRequest.setTableName(tableName(groupId, topic))
    limit.foreach(scanRequest.setLimit(_))

    Try(client.scan(scanRequest).getItems.asScala.map(toLease)) match {
      case Failure(err) =>
        log.error(err, s"AWS Error listing lease for group '$groupId' and topic '$topic':")
        sys.error(s"AWS Error listing lease for group '$groupId' and topic '$topic'")
      case Success(leases) => leases
    }
  }

  override def updateLease(groupId: String, topic: String, lease: UpdateLease): Future[Boolean] = Future {
    val key = new java.util.HashMap[String, AttributeValue]
    key.put(PartitionIdKey, attributeValue(lease.partitionId))

    val request = new UpdateItemRequest()
    request.setTableName(tableName(groupId, topic))
    request.setKey(key)
    request.setExpected(expectedCounterMatch(lease.leaseCounter))

    val newCounter = lease.leaseCounter + 1
    val updateKeys = new util.HashMap[String, AttributeValueUpdate]

    updateKeys.put(LeaseCounterKey, attributeValueUpdate(newCounter))

    lease.checkpointId.fold(updateKeys.put(CheckpointIdKey, attributeValueDelete)) { v =>
      updateKeys.put(CheckpointIdKey, attributeValueUpdate(v))
    }
    lease.leaseHolder.fold(updateKeys.put(LeaseHolderKey, attributeValueDelete)) { v =>
      updateKeys.put(LeaseHolderKey, attributeValueUpdate(v))
    }

    lease.leaseId.fold(updateKeys.put(LeaseIdKey, attributeValueDelete)) { v =>
      updateKeys.put(LeaseIdKey, attributeValueUpdate(v))
    }
    lease.leaseTimestamp.fold(updateKeys.put(LeaseTimestampKey, attributeValueDelete)) { v =>
      updateKeys.put(LeaseTimestampKey, attributeValueUpdate(v.toString))
    }

    request.setAttributeUpdates(updateKeys)

    Try(client.updateItem(request)) match {
      case Failure(conditional: ConditionalCheckFailedException) =>
        val msg =
          s"""
             |Lease failed to update because it did not match set leaseCounter.
             |Current leaseCounter for partitionId: '${lease.partitionId}' group: '$groupId' topic: '$topic' - ${lease.leaseCounter}
           """.stripMargin
        log.info(msg)
        false
      case Failure(err) =>
        log.error(err, s"AWS Error updating lease for group '$groupId' and topic '$topic':")
        sys.error(s"AWS Error updating lease for group '$groupId' and topic '$topic'")
      case Success(_) => true
    }
  }

  override def createLeaseIfNotExists(groupId: String, topic: String, lease: Lease): Future[Boolean] = Future {
    val key = new java.util.HashMap[String, AttributeValue]
    key.put(PartitionIdKey, attributeValue(lease.partitionId))

    val request = new PutItemRequest()
    request.setTableName(tableName(groupId, topic))
    request.setItem(fromLease(lease))
    request.setExpected(expectedNonExist())

    Try(client.putItem(request)) match {
      case Failure(exists: ConditionalCheckFailedException) =>
        val msg =
          s"""
             |Lease failed to create because it already exists.
             |Lease: '$lease' group: '$groupId' topic: '$topic'
           """.stripMargin
        log.info(msg)
        false
      case Failure(err) =>
        log.error(err, s"AWS Error creating lease for group '$groupId' and topic '$topic':")
        sys.error(s"AWS Error creating lease for group '$groupId' and topic '$topic'")
      case Success(_) => true
    }
  }

  override def takeLease(groupId: String, topic: String, lease: Lease): Future[Option[Lease]] = {

    updateLease(groupId, topic, UpdateLease(
      lease.partitionId,
      Option(lease.checkpointId),
      lease.leaseHolder,
      lease.leaseTimestamp,
      lease.leaseCounter,
      Option(lease.leaseId))
    ).map {
      case true => Option(lease.copy(leaseCounter = lease.leaseCounter + 1))
      case false => None
    }
  }

  override def commitLease(groupId: String, topic: String, lease: Lease): Future[Option[Lease]] = {

    updateLease(groupId, topic, UpdateLease(
      lease.partitionId,
      Option(lease.checkpointId),
      lease.leaseHolder,
      lease.leaseTimestamp,
      lease.leaseCounter,
      Option(lease.leaseId))
    ).map {
      case true => Option(lease.copy(leaseCounter = lease.leaseCounter + 1))
      case false => None
    }
  }

  override def renewLease(groupId: String, topic: String, lease: Lease): Future[Option[Lease]] = {

    updateLease(groupId, topic, UpdateLease(
      lease.partitionId,
      Option(lease.checkpointId),
      lease.leaseHolder,
      lease.leaseTimestamp,
      lease.leaseCounter,
      Option(lease.leaseId))
    ).map {
      case true => Option(lease.copy(leaseCounter = lease.leaseCounter + 1))
      case false => None
    }
  }

  override def evictLease(groupId: String, topic: String, lease: Lease): Future[Option[Lease]] = {

    updateLease(groupId, topic, UpdateLease(
      partitionId = lease.partitionId,
      checkpointId = None,
      leaseHolder = None,
      leaseTimestamp = None,
      leaseCounter = lease.leaseCounter,
      leaseId = None
    )).map {
      case true => Option(lease.copy(leaseCounter = lease.leaseCounter + 1))
      case false => None
    }
  }

}
