package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.util._
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, FieldEncode, FieldDecode, DecodeError}
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.analyzer2
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.id
import com.socrata.soda.server.id._
import com.socrata.soda.server.util.CopySpecifier
import com.socrata.soda.server.util.schema.SchemaSpec
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.soda.server.resources.DCCollocateOperation
import com.socrata.thirdparty.json.AdditionalJsonCodecs._
import org.joda.time.DateTime

object DataCoordinatorClient {

  val client = "DC"

  final abstract class MetaTypes extends analyzer2.MetaTypes {
    type ResourceNameScope = Int
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type DatabaseTableNameImpl = (DatasetInternalName, Stage)
    type DatabaseColumnNameImpl = ColumnId
  }

  final abstract class RollupMetaTypes extends analyzer2.MetaTypes {
    type DatabaseTableNameImpl = RollupMetaTypes.TableName

    type ResourceNameScope = MetaTypes#ResourceNameScope
    type ColumnType = MetaTypes#ColumnType
    type ColumnValue = MetaTypes#ColumnValue
    type DatabaseColumnNameImpl = MetaTypes#DatabaseColumnNameImpl
  }

  object RollupMetaTypes {
    // This is basically Either[DatasetInternalName, DatasetResourceName] but it
    // can be used as a map key.  It's temporary until we migrate existing rollups
    // to be all resource-name based
    sealed abstract class TableName
    object TableName {
      case class InternalName(internalName: DatasetInternalName) extends TableName
      case class ResourceName(resourceName: id.ResourceName) extends TableName

      implicit val jCodec = new JsonEncode[TableName] with JsonDecode[TableName] {
        def encode(tableName: TableName) =
          tableName match {
            case InternalName(in) => JsonEncode.toJValue(in)
            case ResourceName(rn) => JsonEncode.toJValue(rn)
          }

        def decode(x: JValue) =
          JsonDecode.fromJValue[DatasetInternalName](x) match {
            case Right(in) =>
              Right(InternalName(in))
            case Left(err1) =>
              JsonDecode.fromJValue[id.ResourceName](x) match {
                case Right(rn) => Right(ResourceName(rn))
                case Left(err2) => Left(DecodeError.join(Seq(err1, err2)))
              }
          }
      }

      implicit def fCodec = new FieldEncode[TableName] with FieldDecode[TableName] {
        def encode(tn: TableName) =
          tn match {
            case InternalName(in) => in.underlying
            case ResourceName(rn) => rn.name
          }

        def decode(x: String) =
          try {
            Right(InternalName(DatasetInternalName(x)))
          } catch {
            case e: IllegalArgumentException =>
              Right(ResourceName(new id.ResourceName(x)))
          }
      }
    }
  }

  @JsonKeyStrategy(Strategy.Underscore)
  case class VersionSpec(raw: Long, shape: Long)
  object VersionSpec {
    implicit val codec = AutomaticJsonCodecBuilder[VersionSpec]
  }

  sealed trait SecondaryValue {}

  @JsonKeyStrategy(Strategy.Underscore)
  case class OnlyVersion(version: Long) extends SecondaryValue
  object  OnlyVersion{
    implicit val codec = WrapperJsonCodec[OnlyVersion].apply[Long](OnlyVersion.apply, _.version)
  }

  @JsonKeyStrategy(Strategy.Underscore)
  case class VersionAndPending(version: Long, pendingDrop: Boolean) extends SecondaryValue
  object  VersionAndPending{
    implicit val codec = AutomaticJsonCodecBuilder[VersionAndPending]
  }

  object SecondaryValue {
    implicit val codec =
    SimpleHierarchyCodecBuilder[SecondaryValue](NoTag)
      .branch[VersionAndPending]
      .branch[OnlyVersion]
      .build
  }

  @JsonKeyStrategy(Strategy.Underscore)
  case class SecondaryVersionsReport(truthInstance: String,
                                     truthVersion: Option[Long], // TODO: remove this once `latestVersion` is not optional and CRJ is no-longer looking for it
                                     latestVersion: Long,
                                     publishedVersion: Option[Long], // TODO: remove once publishedVersions is used everywhere
                                     unpublishedVersion: Option[Long], // TODO: remove once unpublishedVersions is used everywhere
                                     publishedVersions: Option[VersionSpec],
                                     unpublishedVersions: Option[VersionSpec],
                                     secondaries: Map[String, SecondaryValue],
                                     feedbackSecondaries: Set[String],
                                     groups: Map[String, Set[String]],
                                     brokenSecondaries: Option[Map[String, DateTime]] // TODO: make this not an Option once data-coordinator is always sending it
                                    )
  object SecondaryVersionsReport {
    implicit val codec = AutomaticJsonCodecBuilder[SecondaryVersionsReport]
  }

  case class VersionReport(val version: Long)
  object VersionReport{
    implicit val codec = SimpleJsonCodecBuilder[VersionReport].build("version", _.version)
  }

  case class ReportMetaData(val datasetId: DatasetInternalName, val version: Long, val lastModified: DateTime)

  sealed abstract class ReportItem
  case class UpsertReportItem(data: Iterator[JValue] /* Note: this MUST be completely consumed before calling hasNext/next on parent iterator! */) extends ReportItem
  case object OtherReportItem extends ReportItem

  @JsonKeyStrategy(Strategy.Underscore)
  case class Cost(moves: Int, totalSizeBytes: Long, moveSizeMaxBytes: Option[Long] = None)
  object Cost {
    implicit val codec = AutomaticJsonCodecBuilder[Cost]
  }

  @JsonKeyStrategy(Strategy.Underscore)
  case class Move(datasetInternalName: DatasetInternalName,
                  storeIdFrom: String,
                  storeIdTo: String,
                  cost: Cost,
                  complete: Option[Boolean] = None)
  object Move {
    implicit val codec = AutomaticJsonCodecBuilder[Move]
  }

  sealed abstract class Result
  sealed class FailResult extends Result
  sealed class SuccessResult extends Result

  // SUCCESS CASES
  case class NonCreateScriptResult(report: Iterator[ReportItem], etag: Option[EntityTag], copyNumber: Long, newVersion: Long, newShapeVersion: Long, lastModified: DateTime) extends SuccessResult
  case class ExportResult(json: Iterator[JValue], lastModified: Option[DateTime], etag: Option[EntityTag]) extends SuccessResult
  case class RollupResult(rollups: Seq[RollupInfo]) extends SuccessResult
  case class IndexResult(indexes: Seq[IndexInfo]) extends SuccessResult
  case class CollocateResult(jobId : Option[String], status: String, message: String, cost: Cost, moves: Seq[Move]) extends SuccessResult
  case class ResyncResult(secondary: SecondaryId) extends SuccessResult


  // FAIL CASES
  case class SchemaOutOfDateResult(newSchema: SchemaSpec) extends FailResult
  case class NotModifiedResult(etags: Seq[EntityTag]) extends FailResult
  case class IncorrectLifecycleStageResult(actualStage: String, expectedStage: Set[String]) extends FailResult
  case class NoSuchRollupResult(name: RollupName, commandIndex: Long) extends FailResult
  case class InvalidRollupResult(name: RollupName, commandIndex: Long) extends FailResult
  case class NoSuchIndexResult(name: IndexName, commandIndex: Long) extends FailResult
  case class InvalidIndexResult(name: IndexName, commandIndex: Long) extends FailResult
  case object PreconditionFailedResult extends FailResult
  case class InternalServerErrorResult(code: String, tag: String, data: String) extends FailResult
  case class UnexpectedInternalServerResponseResult(reason: String, tag: String) extends FailResult
  case class InvalidLocaleResult(locale: String, commandIndex: Long) extends FailResult
  case object InvalidRowIdResult extends FailResult



  // FAIL CASES: Rows
  case class NoSuchRowResult(id: RowSpecifier, commandIndex: Long) extends FailResult
  case class RowPrimaryKeyNonexistentOrNullResult(id: RowSpecifier, commandIndex: Long) extends FailResult
  case class UnparsableRowValueResult(columnId: ColumnId,tp: String ,value: JValue, commandIndex: Long, commandSubIndex: Long) extends FailResult
  case class RowNoSuchColumnResult(columnId: ColumnId, commandIndex: Long, commandSubIndex: Long) extends FailResult
  case class CannotDeleteRowIdResult(commandIndex: Long) extends FailResult


  // FAIL CASES: Columns
  case class DuplicateValuesInColumnResult(datasetId: DatasetInternalName, columnId: ColumnId, commandIndex: Long) extends FailResult
  case class ColumnExistsAlreadyResult(datasetId: DatasetInternalName, columnId: ColumnId, commandIndex: Long) extends FailResult
  case class IllegalColumnIdResult(columnId: ColumnId, commandIndex: Long) extends FailResult
  case class InvalidSystemColumnOperationResult(datasetId: DatasetInternalName, column: ColumnId, commandIndex: Long) extends FailResult
  case class ColumnNotFoundResult(datasetId: DatasetInternalName, column: ColumnId, commandIndex: Long) extends FailResult

  // FAIL CASES: Datasets
  case class DatasetNotFoundResult(datasetId: DatasetInternalName) extends FailResult
  case class CannotAcquireDatasetWriteLockResult(datasetId: DatasetInternalName) extends FailResult
  case class InitialCopyDropResult(datasetId: DatasetInternalName, commandIndex: Long) extends FailResult
  case class OperationAfterDropResult(datasetId: DatasetInternalName, commandIndex: Long) extends FailResult
  case class FeedbackInProgressResult(datasetId: DatasetInternalName, commandIndex: Long, stores: Set[String]) extends FailResult
  case class DatasetVersionMismatchResult(dataset: DatasetInternalName, version: Long) extends FailResult

  // FAIL CASES: Updates
  case class NotPrimaryKeyResult(datasetId: DatasetInternalName, columnId: ColumnId, commandIndex: Long) extends FailResult
  case class NullsInColumnResult(datasetId: DatasetInternalName, columnId: ColumnId, commandIndex: Long) extends FailResult
  case class InvalidTypeForPrimaryKeyResult(datasetId: DatasetInternalName, columnId: ColumnId,
                                            tp: String, commandIndex: Long) extends FailResult
  case class PrimaryKeyAlreadyExistsResult(datasetId: DatasetInternalName, columnId: ColumnId,
                                           existing: ColumnId, commandIndex: Long) extends FailResult
  case class NoSuchTypeResult(tp: String, commandIndex: Long) extends FailResult
  case class RowVersionMismatchResult(dataset: DatasetInternalName,
                                      value: JValue,
                                      commandIndex: Long,
                                      expected: Option[JValue],
                                      actual: Option[JValue]) extends FailResult
  case class VersionOnNewRowResult(datasetId: DatasetInternalName, commandIndex: Long) extends FailResult
  case class ScriptRowDataInvalidValueResult(datasetId: DatasetInternalName, value: JValue,
                                             commandIndex: Long, commandSubIndex: Long) extends FailResult

  // FAIL CASES: Collocation
  case class InstanceNotExistResult(instance: String) extends FailResult
  case class StoreGroupNotExistResult(storeGroup: String) extends FailResult
  case class StoreDoesNotSupportCollocationResult(storeGroup: String) extends FailResult
  case class StoreNotExistResult(store: String) extends FailResult
  case class DatasetNotExistResult(dataset: DatasetInternalName) extends FailResult
  case class CollocationLockTimeoutResult() extends FailResult

  // FAIL CASES: Resync
  case class DatasetNotInSecondaryResult(secondary: SecondaryId) extends FailResult
}

trait DataCoordinatorClient {
  import DataCoordinatorClient._

  def propagateToSecondary(dataset: DatasetHandle,
                           secondaryId: SecondaryId,
                           secondariesLike: Option[DatasetInternalName])
  def deleteFromSecondary(dataset: DatasetHandle,
                           secondaryId: SecondaryId)
  def getSchema(dataset: DatasetHandle): Option[SchemaSpec]

  def create(resource: ResourceName,
             instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US") : (ReportMetaData, Iterable[ReportItem])

  def update[T](dataset: DatasetHandle,
                schemaHash: String,
                expectedDataVersion: Option[Long],
                user: String,
                instructions: Iterator[DataCoordinatorInstruction])
               (f: Result => T): T

  def copy[T](dataset: DatasetHandle,
              schemaHash: String,
              expectedDataVersion: Option[Long],
              copyData: Boolean,
              user: String,
              instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)
             (f: Result => T): T

  def publish[T](dataset: DatasetHandle,
                 schemaHash: String,
                 expectedDataVersion: Option[Long],
                 user: String,
                 instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)
                (f: Result => T): T

  def dropCopy[T](dataset: DatasetHandle,
                  schemaHash: String,
                  expectedDataVersion: Option[Long],
                  user: String,
                  instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)
                 (f: Result => T): T

  def deleteAllCopies[T](dataset: DatasetHandle,
                         schemaHash: String,
                         expectedDataVersion: Option[Long],
                         user: String)
                        (f: Result => T): T

  def checkVersionInSecondaries(dataset: DatasetHandle): Either[UnexpectedInternalServerResponseResult, Option[SecondaryVersionsReport]]

  def checkVersionInSecondary(dataset: DatasetHandle, secondary: SecondaryId): Either[UnexpectedInternalServerResponseResult, Option[VersionReport]]

  def exportSimple(dataset: DatasetHandle, copy: String, resourceScope: ResourceScope): Result

  def export(dataset: DatasetHandle,
             schemaHash: String,
             columns: Seq[String],
             precondition: Precondition,
             ifModifiedSince: Option[DateTime],
             limit: Option[Long],
             offset: Option[Long],
             copy: String,
             sorted: Boolean,
             rowId: Option[String],
             resourceScope: ResourceScope): Result

  def getRollups(dataset: DatasetHandle): Result
  def getIndexes(dataset: DatasetHandle): Result

  def collocate(secondaryId: SecondaryId, operation: DCCollocateOperation, explain: Boolean, jobId: String): Result
  def collocateStatus(dataset: DatasetHandle, secondaryId: SecondaryId, jobId: String): Result
  def deleteCollocate(dataset: DatasetHandle, secondaryId: SecondaryId, jobId: String): Result

   def resync(dataset: DatasetInternalName, secondaryId: SecondaryId): Result

}
