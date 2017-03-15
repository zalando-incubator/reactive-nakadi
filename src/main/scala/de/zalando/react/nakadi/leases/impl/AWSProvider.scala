package de.zalando.react.nakadi.leases.impl

import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClient}


case class AWSProvider(client: AmazonDynamoDB,
                       readCapacityUnits: Long,
                       writeCapacityUnits: Long,
                       consistentReads: Boolean)

object AWSProvider {

  def apply(): AWSProvider = {

    // TODO read from config
    new AWSProvider(
      client = new AmazonDynamoDBClient(),
      readCapacityUnits = 5,
      writeCapacityUnits = 6,
      consistentReads = false
    )
  }
}
