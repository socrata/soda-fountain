package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.util._
import com.socrata.http.server.util.EntityTag
import com.socrata.soda.server.macros.{Tag, BranchCodec}
import com.socrata.soda.server.id.{RowSpecifier, ColumnId, RollupName, DatasetInternalName}
import com.socrata.soda.server.util.CopySpecifier
import com.socrata.soda.server.util.schema.SchemaSpec


// Single Error Cases

@Tag("mutation.threads.maxed-out")
case class ThreadsMutationError() extends DataCoordinatorError

@Tag("create.dataset.invalid-locale")
case class InvalidLocale(locale: String, commandIndex: Long) extends DataCoordinatorError

@Tag("delete.rollup.does-not-exist")
case class NoSuchRollup(rollup: RollupName, commandIndex: Long) extends DataCoordinatorError

@Tag("create.rollup.invalid")
case class InvalidRollup(rollup: RollupName, commandIndex: Long) extends DataCoordinatorError

// Request Errors

@Tag("req.precondition-failed")
case class PreconditionFailed() extends DCRequestError

// Request Errors: Content-Type
@Tag("req.content-type.request-error")
case class ContentTypeBadRequest(contentTypeError: String) extends DCRequestError

@Tag("req.content-type.missing")
case class ContentTypeMissing() extends DCRequestError

@Tag("req.content-type.unparsable") 
case class ContentTypeUnparsable(`content-type`: String) extends DCRequestError

@Tag("req.content-type.not-json")
case class ContentTypeNotJson(`content-type`: String) extends DCRequestError

@Tag("req.content-type.unknown-charset")
case class ContentTypeUnknownCharset(`content-type`: String) extends DCRequestError

// Request Errors: Export

@Tag("req.export.mismatched-schema")
case class SchemaMismatchForExport(dataset: DatasetInternalName, schema: SchemaSpec) extends DCRequestError


// Request Errors: Body

@Tag("req.body.command-too-large")
case class RequestEntityTooLarge(`bytes-without-full-datum`: Int) extends DCRequestError

@Tag("req.body.malformed-json")
case class BodyMalformedJson(row: Int, column: Int) extends DCRequestError

@Tag("req.body.not-json-array")
case class BodyNotJsonArray() extends DCRequestError

@Tag("req.export.invalid-row-id")
case class InvalidRowId() extends DCRequestError

// Request Errors: Script Header

@Tag("req.script.header.mismatched-schema")
case class SchemaMismatch(dataset: DatasetInternalName, schema: SchemaSpec) extends DCRequestError

@Tag("req.script.header.missing")
case class EmptyCommandStream() extends DCRequestError

@Tag("req.script.header.mismatched-data-version")
case class DatasetVersionMismatch(dataset: DatasetInternalName,
                                  version: Long) extends DCUpdateError

// Request Errors: Script Command

@Tag("req.script.command.non-object")
case class CommandIsNotAnObject(value: JValue, commandIndex: Long) extends DCRequestError

@Tag("req.script.command.missing-field")
case class MissingCommandField(`object`: JObject, field: String, commandIndex: Long) extends DCRequestError

@Tag("req.script.command.invalid-field")
case class InvalidCommandFieldValue(`object`: JObject, field: String, value: JValue, commandIndex: Long) extends DCRequestError


// Update Errors


@Tag("update.read-only-mode")
case class SystemInReadOnlyMode(commandIndex: Long) extends DCUpdateError

@Tag("update.type.unknown")
case class NoSuchType(`type`: String, commandIndex: Long) extends DCUpdateError

@Tag("update.row-version-mismatch")
case class RowVersionMismatch(dataset: DatasetInternalName,
                              value: JValue,
                              commandIndex: Long,
                              expected: Option[JValue],
                              actual: Option[JValue]) extends DCUpdateError

@Tag("update.version-on-new-row")
case class VersionOnNewRow(dataset: DatasetInternalName, commandIndex: Long) extends DCUpdateError

// Update Errors: Column

@Tag("update.column.exists")
case class ColumnAlreadyExists(dataset: DatasetInternalName, column: ColumnId, commandIndex: Long) extends DCColumnUpdateError

@Tag("update.column.illegal-id")
case class IllegalColumnId(id: String, commandIndex: Long) extends DCColumnUpdateError

@Tag("update.column.system")
case class InvalidSystemColumnOperation(dataset: DatasetInternalName, column: ColumnId, commandIndex: Long) extends DCColumnUpdateError

@Tag("update.column.not-found")
case class ColumnNotFound(dataset: DatasetInternalName, column: ColumnId, commandIndex: Long) extends DCColumnUpdateError


// Update Errors: Dataset

@Tag("update.dataset.does-not-exist") // orignally dataset was a string, but not sure why it wouldnt be DatasetId
case class NoSuchDataset(dataset: DatasetInternalName) extends DCDatasetUpdateError

@Tag("update.dataset.temporarily-not-writable")
case class CannotAcquireDatasetWriteLock(dataset: DatasetInternalName) extends DCDatasetUpdateError

@Tag("update.dataset.invalid-state")
case class IncorrectLifecycleStage(dataset: DatasetInternalName,
                                   `actual-state`: String,
                                   `expected-state`: Set[String]) extends DCDatasetUpdateError

@Tag("update.dataset.initial-copy-drop")
case class InitialCopyDrop(dataset: DatasetInternalName, commandIndex: Long) extends DCDatasetUpdateError

@Tag("update.dataset.operation-after-drop")
case class OperationAfterDrop(dataset: DatasetInternalName, commandIndex: Long) extends DCDatasetUpdateError

@Tag("update.dataset.feedback-in-progress")
case class FeedbackInProgress(dataset: DatasetInternalName, commandIndex: Long, stores: Set[String]) extends DCDatasetUpdateError

// Update Errors: Row

@Tag("update.row.primary-key-nonexistent-or-null")
case class RowPrimaryKeyNonexistentOrNull(dataset: DatasetInternalName, commandIndex: Long) extends DCRowUpdateError

@Tag("update.row.no-such-id")
case class NoSuchRow(dataset: DatasetInternalName, value: RowSpecifier, commandIndex: Long) extends DCRowUpdateError

@Tag("update.row.unparsable-value")
case class UnparsableRowValue(dataset: DatasetInternalName, column: ColumnId, `type`: String, value: JValue,
                              commandIndex: Long, commandSubIndex: Long) extends DCRowUpdateError

@Tag("update.row.unknown-column")
case class RowNoSuchColumn(dataset: DatasetInternalName, column: ColumnId, commandIndex: Long, commandSubIndex: Long) extends DCRowUpdateError

// Update Errors: Script Row-Data

@Tag("update.script.row-data.invalid-value")
case class ScriptRowDataInvalidValue(dataset: DatasetInternalName, value: JValue, commandIndex: Long, commandSubIndex: Long) extends DCUpdateError


// Update Errors: Row-Identifier

@Tag("update.row-identifier.already-set")
case class PrimaryKeyAlreadyExists(dataset: DatasetInternalName, column: ColumnId, `existing-column`: ColumnId, commandIndex: Long)
  extends DCUpdateError

@Tag("update.row-identifier.invalid-type")
case class InvalidTypeForPrimaryKey(dataset: DatasetInternalName, column: ColumnId, `type`: String, commandIndex: Long) extends DCUpdateError

@Tag("update.row-identifier.duplicate-values")
case class DuplicateValuesInColumn(dataset: DatasetInternalName, column: ColumnId, commandIndex: Long) extends DCUpdateError

@Tag("update.row-identifier.null-values")
case class NullsInColumn(dataset: DatasetInternalName, column: ColumnId, commandIndex: Long) extends DCUpdateError

@Tag("update.row-identifier.not-row-identifier")
case class NotPrimaryKey(dataset: DatasetInternalName, column: ColumnId, commandIndex: Long) extends DCUpdateError


@Tag("update.row-identifier.delete")
case class DeleteOnRowId(dataset: DatasetInternalName, column: ColumnId, commandIndex: Long) extends DCUpdateError

// Resync Errors

@Tag("resync.dataset.not-in-secondary")
case class DatasetNotInSecondary(secondary: String) extends DCUpdateError

// Collocate Errors

@Tag("collocation.instance.does-not-exist")
case class InstanceNotExist(instance: String) extends DCUpdateError

@Tag("collocation.secondary.store-does-not-support-collocation")
case class StoreDoesNotSupportCollocation(storeGroup: String) extends DCUpdateError

@Tag("collocation.secondary.store-group-does-not-exist")
case class StoreGroupNotExist(storeGroup: String) extends DCUpdateError

@Tag("collocation.secondary.store-does-not-exist")
case class StoreNotExist(store: String) extends DCUpdateError

@Tag("collocation.datataset.does-not-exist")
case class DatasetNotExist(dataset: DatasetInternalName) extends DCUpdateError

@Tag("collocation.lock-timeout")
case class CollocationLockTimeout() extends DCUpdateError

// Unknown Errors not found in Data-Coodinator:

@Tag("req.not-modified")
case class NotModified() extends DataCoordinatorError
// etags are sent in headers


//// Sealed Super Classes
sealed abstract class DCDatasetUpdateError extends PossiblyUnknownDataCoordinatorError {
  def code: String = getClass.getAnnotation(classOf[Tag]).value()
}
object DCDatasetUpdateError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[DCDatasetUpdateError](TagAndValue("errorCode", "data"))).build
}


sealed abstract class DCColumnUpdateError extends PossiblyUnknownDataCoordinatorError {
  def code: String = getClass.getAnnotation(classOf[Tag]).value()
}
object DCColumnUpdateError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[DCColumnUpdateError](TagAndValue("errorCode", "data"))).build
}


sealed abstract class DCRowUpdateError extends PossiblyUnknownDataCoordinatorError {
  def code: String = getClass.getAnnotation(classOf[Tag]).value()
}
object DCRowUpdateError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[DCRowUpdateError](TagAndValue("errorCode", "data"))).build
}


sealed abstract class DCUpdateError extends PossiblyUnknownDataCoordinatorError {
  def code: String = getClass.getAnnotation(classOf[Tag]).value()
}
object DCUpdateError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[DCUpdateError](TagAndValue("errorCode", "data"))).build
}

sealed abstract class DCRequestError extends PossiblyUnknownDataCoordinatorError {
  def code: String = getClass.getAnnotation(classOf[Tag]).value()
}
object DCRequestError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[DCRequestError](TagAndValue("errorCode", "data"))).build
}

sealed abstract class DataCoordinatorError extends PossiblyUnknownDataCoordinatorError {
  def code: String = getClass.getAnnotation(classOf[Tag]).value()
}
object DataCoordinatorError {
  implicit val jCodec = BranchCodec(SimpleHierarchyCodecBuilder[DataCoordinatorError](TagAndValue("errorCode", "data"))).build
}

case class UnknownDataCoordinatorError(errorCode: String, data: Map[String, JValue]) extends PossiblyUnknownDataCoordinatorError
object UnknownDataCoordinatorError {
  implicit val jCodec = AutomaticJsonCodecBuilder[UnknownDataCoordinatorError]
}


sealed abstract class PossiblyUnknownDataCoordinatorError
object PossiblyUnknownDataCoordinatorError {
  implicit val jCodec = SimpleHierarchyCodecBuilder[PossiblyUnknownDataCoordinatorError](NoTag).
    branch[DataCoordinatorError].
    branch[DCRequestError].
    branch[DCUpdateError].
    branch[DCRowUpdateError].
    branch[DCColumnUpdateError].
    branch[DCDatasetUpdateError].
    branch[UnknownDataCoordinatorError].
    build
}
