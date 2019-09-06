package org.broadinstitute.monster.storage.common

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable.IndexedSeq

/** Common definition of file types which can exist in remote storage systems. */
sealed trait FileType extends EnumEntry

object FileType extends Enum[FileType] {
  override val values: IndexedSeq[FileType] = findValues

  case object File extends FileType
  case object Directory extends FileType
  case object Symlink extends FileType
  case object Other extends FileType
}
