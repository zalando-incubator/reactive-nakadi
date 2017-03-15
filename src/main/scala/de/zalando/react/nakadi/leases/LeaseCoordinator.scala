package de.zalando.react.nakadi.leases

import scala.concurrent.Future


trait LeaseCoordinator {

  def workerIdToWorker: Map[String, _]

  def allLeases: Map[String, Lease]

  def pingWorker: Future[Boolean]

  def takeLease: Future[Boolean]

}
