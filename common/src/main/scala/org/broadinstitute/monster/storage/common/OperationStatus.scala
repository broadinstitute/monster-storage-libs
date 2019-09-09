package org.broadinstitute.monster.storage.common

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable.IndexedSeq

/**
  * Convenience data type used to model the stages of performing an incremental
  * operation against some external storage API (i.e. pulling pages of results).
  *
  * Only intended for use within storage-libs, should not be exposed in public APIs.
  */
private[storage] sealed trait OperationStatus extends EnumEntry

private[storage] object OperationStatus extends Enum[OperationStatus] {
  override val values: IndexedSeq[OperationStatus] = findValues

  private[storage] case object NotStarted extends OperationStatus
  private[storage] case class InProgress(token: String) extends OperationStatus
  private[storage] case object Done extends OperationStatus
}
