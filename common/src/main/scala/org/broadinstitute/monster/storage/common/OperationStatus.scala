package org.broadinstitute.monster.storage.common

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable.IndexedSeq

private[storage] sealed trait OperationStatus extends EnumEntry

private[storage] object OperationStatus extends Enum[OperationStatus] {
  override val values: IndexedSeq[OperationStatus] = findValues

  case object NotStarted extends OperationStatus
  case class InProgress(token: String) extends OperationStatus
  case object Done extends OperationStatus
}
