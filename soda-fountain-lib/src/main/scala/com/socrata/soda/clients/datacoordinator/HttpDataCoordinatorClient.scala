package com.socrata.soda.clients.datacoordinator

import scala.annotation.tailrec
import java.io.IOException
import java.net.URLEncoder
import com.rojoma.json.v3.ast.{JNull, JValue}
import com.rojoma.json.v3.io._
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AutomaticJsonEncodeBuilder, JsonArrayIterator}
import com.rojoma.simplearm.v2._
import com.socrata.http.client.exceptions.{FullTimeout, UnexpectedContentType, ConnectFailed, ConnectTimeout}
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.common.util.HttpUtils
import com.socrata.http.server.implicits._
import com.socrata.http.server.routing.HttpMethods
import com.socrata.http.server.util._
import com.socrata.soda.server.{ThreadLimiter, SodaUtils}
import com.socrata.soda.server.id._
import com.socrata.soda.server.util.schema.SchemaSpec
import javax.servlet.http.HttpServletResponse
import com.socrata.soda.server.highlevel.ColumnDAO.DatasetNotFound
import com.socrata.soda.server.resources.DCCollocateOperation
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import com.socrata.http.client.BodylessHttpRequest


abstract class HttpDataCoordinatorClient extends DataCoordinatorClient {
  import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient._

  val httpClient: HttpClient
  val threadLimiter: ThreadLimiter

  val log = org.slf4j.LoggerFactory.getLogger(classOf[DataCoordinatorClient])

  val dateTimeParser = ISODateTimeFormat.dateTimeParser
  val xhDataVersion = "X-SODA2-Truth-Version"
  val xhDataShapeVersion = "X-SODA2-Truth-Shape-Version"
  val xhLastModified = "X-SODA2-Truth-Last-Modified"
  val xhCopyNumber = "X-SODA2-Truth-Copy-Number"

  private implicit class AugmentedRequestBuilder(underlying: RequestBuilder) {
    def addResourceHeader(handle: DatasetHandle) =
      underlying.addHeader(resourceHeader(handle))
  }

  def hostO(instance: String): Option[RequestBuilder]
  def instances(): Set[String]
  def createReq(host: RequestBuilder) = host.p("dataset")
  def mutateReq(host: RequestBuilder, dataset: DatasetHandle) =
    host.p("dataset", dataset.datasetId.underlying).
      addResourceHeader(dataset)
  def schemaReq(host: RequestBuilder, dataset: DatasetHandle) =
    host.p("dataset", dataset.datasetId.underlying, "schema").
      addResourceHeader(dataset)
  def secondariesReq(host: RequestBuilder, dataset: DatasetHandle) =
    host.p("secondaries-of-dataset", dataset.datasetId.underlying).
      addResourceHeader(dataset)
  def secondaryReq(host: RequestBuilder, secondaryId: SecondaryId, dataset: DatasetHandle) =
    host.p("secondary-manifest", secondaryId.underlying, dataset.datasetId.underlying).
      addResourceHeader(dataset)
  def exportReq(host: RequestBuilder, dataset: DatasetHandle) =
    host.p("dataset", dataset.datasetId.underlying).
      addResourceHeader(dataset)
  def rollupReq(host: RequestBuilder, dataset: DatasetHandle) =
    host.p("dataset-rollup", dataset.datasetId.underlying).
      addResourceHeader(dataset)
  def indexReq(host: RequestBuilder, dataset: DatasetHandle) =
    host.p("dataset-index", dataset.datasetId.underlying).
      addResourceHeader(dataset)
  def collocateUrl(host: RequestBuilder, secondaryId: SecondaryId) =
    host.p("secondary-manifest", secondaryId.underlying, "collocate")
  def collocateStatusReq(host: RequestBuilder, secondaryId: SecondaryId, jobId: String) =
    host.p("secondary-manifest", "move", secondaryId.underlying, "job", jobId)
  def collocateJobReq(host: RequestBuilder, jobId: String) =
    host.p("secondary-move-jobs", "job", jobId)
  def resyncReq(host: RequestBuilder, dataset: DatasetInternalName, secondaryId: SecondaryId) =
    host.p("resync", dataset.underlying, secondaryId.underlying)

  @tailrec
  final def withConnectRetries[T](retriesRemaining: Int = 5)(f: => T): T = {
    try {
      return f
    } catch {
      case _ : ConnectFailed if retriesRemaining > 0 =>
        // fall out of the catch
      case _ : ConnectTimeout if retriesRemaining > 0 =>
        // fall out of the catch
    }
    withConnectRetries(retriesRemaining - 1)(f)
  }

  @tailrec
  final def withFullRetries[T](retriesRemaining: Int = 5)(f: => T): T = {
    try {
      return f
    } catch {
      case _ : ConnectFailed if retriesRemaining > 0 =>
        // fall out of the catch
      case _ : ConnectTimeout if retriesRemaining > 0 =>
        // fall out of the catch
      case _: IOException if retriesRemaining > 0 =>
        // fall out of the catch
    }
    withFullRetries(retriesRemaining - 1)(f)
  }

  def withHost[T](instance: String)(f: RequestBuilder => T): T = {
    hostO(instance) match {
      case Some(host) =>
        // The vast majority of data-coordinator requests come through here
        threadLimiter.withThreadpool {
          f(host)
        }
      case None => throw new Exception(s"could not find data coordinator for instance ${instance}")
    }
  }

  def withHost[T](datasetId: DatasetInternalName, mutate: Boolean)(f: RequestBuilder => T): T = {
    var instance = datasetId.nativeDataCoordinator
    if(mutate) instance += ".mutation"
    withHost(instance)(f)
  }

  def withHost[T](datasetHandle: DatasetHandle, mutate: Boolean)(f: RequestBuilder => T): T =
    withHost(datasetHandle.datasetId, mutate)(f)

  def propagateToSecondary(dataset: DatasetHandle,
                           secondaryId: SecondaryId,
                           secondariesLike: Option[DatasetInternalName]): Unit =
    withConnectRetries() {
      withHost(dataset.datasetId, mutate = false) { host =>
        val sr = secondaryReq(host, secondaryId, dataset)
        val sr2 = secondariesLike match {
          case Some(datasetId) => sr.addParameter(("secondaries_like", datasetId.underlying))
          case None => sr
        }
        val r = sr2.jsonBody(JNull)
        httpClient.execute(r).run { response =>
          response.resultCode match {
            case HttpServletResponse.SC_OK => // ok
            case _ => throw new Exception(s"could not propagate to secondary ${secondaryId}")
          }
        }
      }
    }

  def deleteFromSecondary(dataset: DatasetHandle,
                           secondaryId: SecondaryId): Unit =
    withConnectRetries() {
      withHost(dataset.datasetId, mutate = false) { host =>
        val request = secondaryReq(host, secondaryId, dataset).delete
        httpClient.execute(request).run { response =>
          response.resultCode match {
            case HttpServletResponse.SC_OK => // ok
            case HttpServletResponse.SC_NOT_FOUND => // ok
            case _ => throw new Exception(s"could not delete from secondary ${secondaryId}")
          }
        }
      }
    }

  implicit class Augmenting(r: RequestBuilder) {
    def precondition(p: Precondition): RequestBuilder = r.addHeaders(PreconditionRenderer(p))
  }

  def headerExists(header: String, resp: Response) = resp.headerNames.contains(header.toLowerCase)

  def getHeader(header: String, resp: Response) = resp.headers(header)(0)

  def getSchema(dataset: DatasetHandle): Option[SchemaSpec] = {
    withFullRetries() {
      withHost(dataset, mutate = false) { host =>
        val request = schemaReq(host, dataset).get
        httpClient.execute(request).run { response =>
          response.resultCode match {
            case HttpServletResponse.SC_OK => {
              response.value[SchemaSpec]() match {
                case Right(jv) => Some(jv)
                case Left(_) => throw new Exception("Unable to interpret data coordinator's response for " + dataset + " as a schemaspec?")
              }
            }
            case HttpServletResponse.SC_NOT_FOUND => None
            case _ => throw new Exception("Unexpected result from server: " + response.resultCode)
          }
        }
      }
    }
  }

  def errorFrom[T](r: Response): Option[PossiblyUnknownDataCoordinatorError] =
    r.resultCode match {
      case HttpServletResponse.SC_OK =>
        None
      // unclear why this is a special case - answer: because it doesn't have a JSON body
      case HttpServletResponse.SC_NOT_MODIFIED =>
        Some(NotModified())
      case code =>
        try {
          Some(r.value[PossiblyUnknownDataCoordinatorError]().right.toOption.getOrElse(
            throw new Exception(s"Response was JSON but not decodable as an error - code $code")))
        } catch {
          case e: UnexpectedContentType =>
            throw new Exception(s"Data coordinator gave unexpected response of status $code and content-type ${r.contentType}.")
        }
    }

  def expectStartOfArray(in: Iterator[JsonEvent]) {
    if(!in.hasNext) throw new JsonParserEOF(Position.Invalid)
    val t = in.next()
    if(!t.isInstanceOf[StartOfArrayEvent]) throw new JsonBadParse(t)
  }

  // TODO: This skip-next-datum stuff should be in a library.
  // Specifically it should be in rojoma-json.
  def skipNextDatum(in: BufferedIterator[JsonEvent]) {
    def skipArray(in: BufferedIterator[JsonEvent]) {
      while(!in.head.isInstanceOf[EndOfArrayEvent]) skipNextDatum(in)
      in.next()
    }

    def skipObject(in: BufferedIterator[JsonEvent]) {
      while(!in.head.isInstanceOf[EndOfObjectEvent]) skipNextDatum(in)
      in.next()
    }

    in.next() match {
      case StartOfArrayEvent() => skipArray(in)
      case StartOfObjectEvent() => skipObject(in)
      case _ => // nothing
    }
  }

  def arrayOfResults(in: BufferedIterator[JsonEvent], alreadyInArray: Boolean = false): Iterator[ReportItem] = {
    if(!alreadyInArray) expectStartOfArray(in)
    new Iterator[ReportItem] {
      var pendingIterator: Iterator[JValue] = null
      def invalidatePending() {
        if((pendingIterator ne null) && pendingIterator.hasNext) {
          throw new Exception("Row result was not completely consumed")
        }
        pendingIterator = null
      }
      def hasNext = { invalidatePending(); in.hasNext && !in.head.isInstanceOf[EndOfArrayEvent] }
      def next(): ReportItem = {
        if(!hasNext) Iterator.empty.next()
        if(in.head.isInstanceOf[StartOfArrayEvent]) {
          pendingIterator = JsonArrayIterator.fromEvents[JValue](in)
          UpsertReportItem(pendingIterator)
        } else {
          skipNextDatum(in)
          OtherReportItem
        }
      }
    }
  }

  /**
   * For cases where we will be running mutations in D.C. Creates an http-post request if method has not yet been defined.
   * @param rb
   * @param script
   * @param f
   * @tparam T
   * @return
   */
  protected def sendScript[T](rb: RequestBuilder, script: MutationScript)(f: Either[Result, Response] => T): T = {
    val request = rb.json(script.it)
    httpClient.execute(request).run { r =>
      errorFrom(r) match {
        case None =>
          if (headerExists(xhDataVersion, r) && headerExists(xhLastModified, r))
            f(Right(r))
          else {
            log.error("No version headers set from data coordinator")
            throw new Exception("No version headers set from data coordinator")
          }
        case Some(err) =>
          err match {
            case(reqError: DCRequestError) => reqError match {
                case ContentTypeBadRequest(contentErrorType) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, contentErrorType)))
                case PreconditionFailed() =>
                  f(Left(PreconditionFailedResult))
                case ContentTypeMissing() =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, "")))
                case ContentTypeUnparsable(contentType) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, contentType)))
                case ContentTypeNotJson(contentType) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, contentType)))
                case ContentTypeUnknownCharset(contentType) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, contentType)))
                case SchemaMismatchForExport(dataset, schema) =>
                  f(Left(SchemaOutOfDateResult(schema)))
                case RequestEntityTooLarge(bytes) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, bytes.toString)))
                case BodyMalformedJson(row, column) =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, s"row $row; column $column")))
                case BodyNotJsonArray() =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, "")))
                case SchemaMismatch(dataset, schema) =>
                  f(Left(SchemaOutOfDateResult(schema)))
                case EmptyCommandStream() =>
                  f(Left(InternalServerErrorResult(reqError.code, tag, "")))
                case CommandIsNotAnObject(value, commandIndex) =>
                  f(Left(InternalServerErrorResult( reqError.code, tag, s"value: $value, commandIndex: $commandIndex")))
                case MissingCommandField(obj, field, commandIndex) =>
                  f(Left(InternalServerErrorResult( reqError.code, tag, s"obj: $obj, field: $field, commandIndex: $commandIndex" )))
                case InvalidCommandFieldValue(obj, field, value, commandIndex) =>
                  f(Left(InternalServerErrorResult( reqError.code, tag, s"obj: $obj, field: $field, value: $value, commandIndex: $commandIndex")))
                case InvalidRowId() =>
                  f(Left(InvalidRowIdResult))
            }
            case (rowError: DCRowUpdateError) => rowError match {
              case RowPrimaryKeyNonexistentOrNull(dataset, commandIndex) =>
                f(Left(RowPrimaryKeyNonexistentOrNullResult(RowSpecifier(""), commandIndex)))
              case NoSuchRow(_, id, commandIndex) =>
                f(Left(NoSuchRowResult(id, commandIndex)))
              case UnparsableRowValue(_, column, tp, value, commandIndex, commandSubIndex) =>
                f(Left(UnparsableRowValueResult(column, tp, value, commandIndex, commandSubIndex)))
              case RowNoSuchColumn(dataset, column, commandIndex, commandSubIndex) =>
                f(Left(RowNoSuchColumnResult(column, commandIndex, commandSubIndex)))
            }
            case (columnError: DCColumnUpdateError) => columnError match {
              case ColumnAlreadyExists(dataset, column, commandIndex) =>
                f(Left(ColumnExistsAlreadyResult(dataset, column, commandIndex)))
              case IllegalColumnId(id, commandIndex) =>
                f(Left(IllegalColumnIdResult(ColumnId(id), commandIndex)))
              case InvalidSystemColumnOperation(dataset, column, commandIndex) =>
                f(Left(InvalidSystemColumnOperationResult(dataset, column, commandIndex)))
              case ColumnNotFound(dataset, column, commandIndex) =>
                f(Left(ColumnNotFoundResult(dataset, column, commandIndex)))
            }
            case (datasetError: DCDatasetUpdateError) => datasetError match {
              case NoSuchDataset(dataset) =>
                f(Left(DatasetNotFoundResult(dataset)))
              case CannotAcquireDatasetWriteLock(dataset) =>
                f(Left(CannotAcquireDatasetWriteLockResult(dataset)))
              case IncorrectLifecycleStage(dataset, actualStage, expectedStage) =>
                f(Left(IncorrectLifecycleStageResult(actualStage, expectedStage)))
              case InitialCopyDrop(dataset, commandIndex) =>
                f(Left(InitialCopyDropResult(dataset, commandIndex)))
              case OperationAfterDrop(dataset, commandIndex) =>
                f(Left(OperationAfterDropResult(dataset, commandIndex)))
              case FeedbackInProgress(dataset, commandIndex, stores) =>
                f(Left(FeedbackInProgressResult(dataset, commandIndex, stores)))
            }
            case (updateError: DCUpdateError) => updateError match {
              case SystemInReadOnlyMode(commandIndex) =>
                f(Left(InternalServerErrorResult(updateError.code, tag, s"commandIndex: $commandIndex")))
              case NoSuchType(tp, commandIndex) =>
                f(Left(NoSuchTypeResult(tp, commandIndex)))
              case DatasetVersionMismatch(dataset, version) =>
                f(Left(DatasetVersionMismatchResult(dataset, version)))
              case RowVersionMismatch(dataset, value, commandIndex, expected, actual) =>
                f(Left(RowVersionMismatchResult(dataset, value, commandIndex, expected, actual)))
              case VersionOnNewRow(dataset, commandIndex) =>
                f(Left(VersionOnNewRowResult(dataset, commandIndex)))
              case ScriptRowDataInvalidValue(dataset, value, commandIndex, commandSubIndex) =>
                f(Left(ScriptRowDataInvalidValueResult(dataset, value, commandIndex, commandSubIndex)))
              case PrimaryKeyAlreadyExists(dataset, column, existingColumn, commandIndex) =>
                f(Left(PrimaryKeyAlreadyExistsResult(dataset, column, existingColumn, commandIndex)))
              case InvalidTypeForPrimaryKey(dataset, column, tp, commandIndex) =>
                f(Left(InvalidTypeForPrimaryKeyResult(dataset, column, tp, commandIndex)))
              case DuplicateValuesInColumn(dataset, column, commandIndex) =>
                f(Left(DuplicateValuesInColumnResult(dataset, column, commandIndex)))
              case NullsInColumn(dataset, column, commandIndex) =>
                f(Left(NullsInColumnResult(dataset, column, commandIndex)))
              case NotPrimaryKey(dataset, column, commandIndex) =>
                f(Left(NotPrimaryKeyResult(dataset, column, commandIndex)))
              case DeleteOnRowId(dataset, column, commandIndex) =>
                f(Left(CannotDeleteRowIdResult(commandIndex)))  // commandIndex available
              case DatasetNotExist(dataset) =>
                f(Left(DatasetNotExistResult(dataset)))
              case InstanceNotExist(instance) =>
                f(Left(InstanceNotExistResult(instance)))
              case StoreGroupNotExist(group) =>
                f(Left(StoreGroupNotExistResult(group)))
              case StoreNotExist(store) =>
                f(Left(StoreNotExistResult(store)))

            }
            case (dcError: DataCoordinatorError) => dcError match {
              case ThreadsMutationError() =>
                f(Left(InternalServerErrorResult(dcError.code, tag, "")))
              case InvalidLocale(locale, commandIndex) =>
                f(Left(InvalidLocaleResult(locale, commandIndex)))
              case NoSuchRollup(name, commandIndex) =>
                f(Left(NoSuchRollupResult(name, commandIndex)))
              case InvalidRollup(name, commandIndex) =>
                f(Left(InvalidRollupResult(name, commandIndex)))
              case NotModified() =>
                f(Left(NotModifiedResult(etagsSeq(r))))
            }
            case UnknownDataCoordinatorError(code, data) =>
              log.error("Unknown data coordinator error: code %s, Aux info: %s".format(code, data))
              f(Left(InternalServerErrorResult(code, tag, data.mkString(","))))
          }
      }
    }
  }


  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }

  def sendNonCreateScript[T](rb: RequestBuilder, script: MutationScript)(f: Result => T): T =
    sendScript(rb, script) {
      case Right(r) =>
        f(NonCreateScriptResult(
            arrayOfResults(r.jsonEvents().buffered),
            None,
            getHeader(xhCopyNumber, r).toLong,
            getHeader(xhDataVersion, r).toLong,
            if(headerExists(xhDataShapeVersion, r)) getHeader(xhDataShapeVersion, r).toLong else getHeader(xhDataVersion, r).toLong,
            dateTimeParser.parseDateTime(getHeader(xhLastModified, r))))
      case Left(e) => f(e)
    }

  def create(resource: ResourceName,
             instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US"): (ReportMetaData, Iterable[ReportItem]) = {
    withConnectRetries() {
      withHost(instance) { host =>
        val createScript = new MutationScript(user, CreateDataset(resource, locale), instructions.getOrElse(Array().iterator))
        sendScript(createReq(host), createScript) {
          case Right(r) =>
            val events = r.jsonEvents().buffered
            expectStartOfArray(events)
            if (!events.hasNext || !events.head.isInstanceOf[StringEvent])
              throw new Exception("Bad response from data coordinator: expected dataset id")
            val StringEvent(datasetId) = events.next()
            (ReportMetaData(DatasetInternalName(datasetId),
                            getHeader(xhDataVersion, r).toLong,
                            dateTimeParser.parseDateTime(getHeader(xhLastModified, r))),
             arrayOfResults(events, alreadyInArray = true).toSeq)
          case other =>
            throw new Exception("Unexpected response from data-coordinator: " + other)
        }
      }
    }
  }

  def update[T](dataset: DatasetHandle,
                schemaHash: String,
                expectedDataVersion: Option[Long],
                user: String,
                instructions: Iterator[DataCoordinatorInstruction])
               (f: Result => T): T = {
    // TODO: update should decode the row op report into something higher-level than JValues
    withConnectRetries() {
      withHost(dataset, mutate = true) { host =>
        val updateScript = new MutationScript(user, UpdateDataset(schemaHash, expectedDataVersion), instructions)
        sendNonCreateScript(mutateReq(host, dataset), updateScript)(f)
      }
    }
  }

  def copy[T](dataset: DatasetHandle,
              schemaHash: String,
              expectedDataVersion: Option[Long],
              copyData: Boolean,
              user: String,
              instructions: Iterator[DataCoordinatorInstruction])
             (f: Result => T): T = {
    // TODO: copy should decode the row op report into something higher-level than JValues
    withConnectRetries() {
      withHost(dataset, mutate = true) { host =>
        val createScript = new MutationScript(user, CopyDataset(copyData, schemaHash, expectedDataVersion), instructions)
        sendNonCreateScript(mutateReq(host, dataset), createScript)(f)
      }
    }
  }

  def publish[T](dataset: DatasetHandle,
                 schemaHash: String,
                 expectedDataVersion: Option[Long],
                 user: String,
                 instructions: Iterator[DataCoordinatorInstruction])
                (f: Result => T): T = {
    // TODO: publish should decode the row op report into something higher-level than JValues
    withConnectRetries() {
      withHost(dataset, mutate = true) { host =>
        val pubScript = new MutationScript(user, PublishDataset(schemaHash, expectedDataVersion), instructions)
        sendNonCreateScript(mutateReq(host, dataset), pubScript)(f)
      }
    }
  }

  def dropCopy[T](dataset: DatasetHandle,
                  schemaHash: String,
                  expectedDataVersion: Option[Long],
                  user: String,
                  instructions: Iterator[DataCoordinatorInstruction])
                 (f: Result => T): T = {
    // TODO: dropCopy should decode the row op report into something higher-level than JValues
    withConnectRetries() {
      withHost(dataset, mutate = true) { host =>
        val dropScript = new MutationScript(user, DropDataset(schemaHash, expectedDataVersion), instructions)
        sendNonCreateScript(mutateReq(host, dataset), dropScript)(f)
      }
    }
  }

  // Pretty sure this is completely wrong
  def deleteAllCopies[T](dataset: DatasetHandle,
                         schemaHash: String,
                         expectedDataVersion: Option[Long],
                         user: String)
                        (f: Result => T): T = {
    // TODO: deleteAllCopies should decode the row op report into something higher-level than JValues
    withConnectRetries() {
      withHost(dataset, mutate = true) { host =>
        val deleteScript = new MutationScript(user, DropDataset(schemaHash, expectedDataVersion), Iterator.empty)
        val req = mutateReq(host, dataset).method(HttpMethods.DELETE)
        sendNonCreateScript(req, deleteScript)(f)
      }
    }
  }

  val UninterpretableDataCoordinatorResponseBody = "uninterpretable response body from data-coordinator"
  def UnexpectedDataCoordinatorResponseCode(code: Int) = s"unexpected response code from data-coordinator: $code"

  def checkVersionInSecondaries(dataset: DatasetHandle): Either[UnexpectedInternalServerResponseResult, Option[SecondaryVersionsReport]] = {
    withFullRetries() {
      withHost(dataset, mutate = false) { host =>
        val request = secondariesReq(host, dataset)
          .addHeader(("Content-type", "application/json"))
          .timeoutMS(10000)
          .get
        try {
          httpClient.execute(request).run { response =>
            response.resultCode match {
              case HttpServletResponse.SC_OK =>
                val oVers = response.value[SecondaryVersionsReport]()
                oVers match {
                  case Right(vers) => Right(Some(vers))
                  case Left(other) =>
                    val reason = UninterpretableDataCoordinatorResponseBody
                    log.error(s"{}: {}", reason: Any, other.english)
                    Left(UnexpectedInternalServerResponseResult(reason, tag))
                }
              case HttpServletResponse.SC_NOT_FOUND => Right(None)

              case other =>
                val reason = UnexpectedDataCoordinatorResponseCode(other)
                log.error(reason)
                Left(UnexpectedInternalServerResponseResult(reason, tag))
            }
          }
        } catch {
          case e: FullTimeout =>
            log.error("DC Version request timed out")
            throw e
        }
      }
    }
  }

  def resourceHeader(dataset: DatasetHandle) =
    SodaUtils.ResourceHeader -> URLEncoder.encode(dataset.resourceName.name, "UTF-8")

  def checkVersionInSecondary(dataset: DatasetHandle,
                              secondaryId: SecondaryId): Either[UnexpectedInternalServerResponseResult, Option[VersionReport]] = {
    withFullRetries() {
      withHost(dataset, mutate = false) { host =>
        val request = secondaryReq(host, secondaryId, dataset)
          .addHeader(("Content-type", "application/json"))
          .addHeader(resourceHeader(dataset))
          .timeoutMS(10000)
          .get
        httpClient.execute(request).run { response =>
          response.resultCode match {
            case HttpServletResponse.SC_OK =>
              val oVer = response.value[VersionReport]()
              oVer match {
                case Right(ver) => Right(Some(ver))
                case Left(other) =>
                  val reason = UninterpretableDataCoordinatorResponseBody
                  log.error(s"{}: {}", reason: Any, other.english)
                  Left(UnexpectedInternalServerResponseResult(reason, tag))
              }
            case HttpServletResponse.SC_NOT_FOUND => Right(None)
            case other =>
              val reason = UnexpectedDataCoordinatorResponseCode(other)
              log.error(reason)
              Left(UnexpectedInternalServerResponseResult(reason, tag))
          }
        }
      }
    }
  }

  private def convertExportError(r: Response, err: PossiblyUnknownDataCoordinatorError): FailResult =
    err match {
      case SchemaMismatchForExport(_, newSchema) =>
        SchemaOutOfDateResult(newSchema)
      case NotModified() =>
        NotModifiedResult(etagsSeq(r))
      case PreconditionFailed() =>
        PreconditionFailedResult
      case NoSuchDataset(dataset) =>
        DatasetNotFoundResult(dataset)
      case cbr: ContentTypeBadRequest =>
        InternalServerErrorResult(cbr.code, tag, cbr.contentTypeError)
      case InvalidRowId() =>
        InvalidRowIdResult
      case x: DataCoordinatorError =>
        InternalServerErrorResult(x.code, tag, "")
      case UnknownDataCoordinatorError(code, data) =>
        log.error("Unknown data coordinator error: code %s, Aux info: %s".format(code, data))
        InternalServerErrorResult(code, tag, data.mkString(","))
      case x =>
        log.warn("case is NOT implemented %s".format(x.toString))
        InternalServerErrorResult("unknown", tag, x.toString)
    }

  def exportSimple(dataset: DatasetHandle, copy: String, resourceScope: ResourceScope) = {
    withConnectRetries() {
      withHost(dataset, mutate = false) { host =>
        val request = exportReq(host, dataset)
          .addParameter("copy" -> copy)
          .get
        using(new ResourceScope()) { tmpScope =>
          val r = httpClient.execute(request, tmpScope)
          errorFrom(r) match {
            case None =>
              val etag = r.headers("ETag").headOption.map(EntityTagParser.parse(_))
              val lastModified = r.headers("Last-Modified").headOption.map(HttpUtils.parseHttpDate)
              tmpScope.transfer(r).to(resourceScope)
              val array = resourceScope.openUnmanaged(r.array[JValue](), transitiveClose = List(r))
              ExportResult(array, lastModified, etag)
            case Some(err) =>
              convertExportError(r, err)
          }
        }
      }
    }
  }

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
             resourceScope: ResourceScope): Result = {
    withConnectRetries() {
      withHost(dataset, mutate = false) { host =>
        val limParam = limit.map { limLong => "limit" -> limLong.toString }
        val offParam = offset.map { offLong => "offset" -> offLong.toString }
        val columnsParam = if (columns.isEmpty) None else Some("c" -> columns.mkString(","))
        val rowIdParam = if (rowId.isEmpty) None else rowId.map("row_id" -> _)
        val sortedParam = "sorted" -> sorted.toString
        val request = exportReq(host, dataset)
                      .q("schemaHash" -> schemaHash)
                      .addParameter("copy"->copy)
                      .addParameters(limParam ++ offParam ++ columnsParam ++ rowIdParam)
                      .addParameter(sortedParam)
                      .addHeaders(PreconditionRenderer(precondition) ++ ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate))
                      .get
        using(new ResourceScope()) { tmpScope =>
          val r = httpClient.execute(request, tmpScope)
          errorFrom(r) match {
            case None =>
              val etag = r.headers("ETag").headOption.map(EntityTagParser.parse(_))
              val lastModified = r.headers("Last-Modified").headOption.map(HttpUtils.parseHttpDate)
              tmpScope.transfer(r).to(resourceScope)
              val array = resourceScope.openUnmanaged(r.array[JValue](), transitiveClose = List(r))
              ExportResult(array, lastModified, etag)
            case Some(err) =>
              convertExportError(r, err)
          }
        }
      }
    }
  }

  override def getRollups(dataset: DatasetHandle): Result = {
    withFullRetries() {
      withHost(dataset, mutate = false) { host =>
        val request = rollupReq(host, dataset).get
        httpClient.execute(request).run { r =>
          errorFrom(r) match {
            case None => r.value[Seq[RollupInfo]]() match {
              case Right(resp) => RollupResult(resp)
              case Left(err) => throw new Exception("Unable to parse response from data coordinator: " + err.english)
            }
            case Some(NoSuchDataset(dataset)) => DatasetNotFoundResult(dataset)
            case Some(err) =>
              throw new Exception(s"Unexpected error from data-coordinator getting rollups for dataset $dataset: $err")
          }
        }
      }
    }
  }

  override def getIndexes(dataset: DatasetHandle): Result = {
    withFullRetries() {
      withHost(dataset, mutate = false) { host =>
        val request = indexReq(host, dataset).get
        httpClient.execute(request).run { r =>
          errorFrom(r) match {
            case None => r.value[Seq[IndexInfo]]() match {
              case Right(resp) => IndexResult(resp)
              case Left(err) => throw new Exception("Unable to parse response from data coordinator: " + err.english)
            }
            case Some(NoSuchDataset(dataset)) => DatasetNotFoundResult(dataset)
            case Some(err) =>
              throw new Exception(s"Unexpected error from data-coordinator getting indexes for dataset $dataset: $err")
          }
        }
      }
    }
  }

  private def collocateResult(response: Response): CollocateResult = {
    case class CollocateResponse(id: Option[String],
                                 status: String,
                                 message: String,
                                 cost: Cost,
                                 moves: Seq[Move])

    implicit val cCodec = AutomaticJsonCodecBuilder[CollocateResponse]

    response.value[CollocateResponse]() match {
      // NOTE: data-coordinator response has the field "id" where core expects "jobId"
      case Right(resp) => CollocateResult(
        resp.id,
        resp.status,
        resp.message,
        resp.cost,
        resp.moves
      )
      case Left(e) => throw new Exception("Unable to parse response from data coordinator: " + e.english)
    }
  }

  override def collocate(secondaryId: SecondaryId, operation: DCCollocateOperation, explain: Boolean, jobId: String): Result = {
    implicit val encoder = AutomaticJsonEncodeBuilder[DCCollocateOperation]

    // Use any of the dcs mentioned in the operation as a host
    withConnectRetries() {
      withHost(operation.collocations.head.head, mutate = false) { host =>
        val request = collocateUrl(host, secondaryId)
          .addParameter("explain" -> explain.toString)
          .addParameter("job" -> jobId)
          .jsonBody[DCCollocateOperation](operation)
        httpClient.execute(request).run { r =>
          errorFrom(r) match {
            case None => collocateResult(r)
            //NOTE: These are duplicated in sendScript, is there any way to prevent this?
            case Some(InstanceNotExist(instance)) => InstanceNotExistResult(instance)
            case Some(StoreGroupNotExist(storeGroup)) => StoreGroupNotExistResult(storeGroup)
            case Some(StoreDoesNotSupportCollocation(storeGroup)) => StoreDoesNotSupportCollocationResult(storeGroup)
            case Some(StoreNotExist(store)) => StoreNotExistResult(store)
            case Some(DatasetNotExist(dataset)) => DatasetNotExistResult(dataset)
            case Some(CollocationLockTimeout()) => CollocationLockTimeoutResult()
            case Some(e) =>
              throw new Exception("Unexpected error from data-coordinator on collocation: " + e)
          }
        }
      }
    }
  }

  override def collocateStatus(dataset: DatasetHandle, secondaryId: SecondaryId, jobId: String): Result = {
    withFullRetries() {
      withHost(dataset, mutate = false) { host =>
        val request = collocateStatusReq(host, secondaryId, jobId).get
        httpClient.execute(request).run { r =>
          errorFrom(r) match {
            case None => collocateResult(r)
            case Some(StoreGroupNotExist(storeGroup)) => StoreGroupNotExistResult(storeGroup)
            case Some(e) =>
              throw new Exception("Unexpected error from data-coordinator on collocation status: " + e)
          }
        }
      }
    }
  }

  override def deleteCollocate(dataset: DatasetHandle, secondaryId: SecondaryId, jobId: String): Result = {
    withConnectRetries() {
      withHost(dataset, mutate = false) { host =>
        val request = collocateJobReq(host, jobId).delete
        httpClient.execute(request).run { r =>
          errorFrom(r) match {
            case None => CollocateResult(Some(jobId), "deleted", "deleted", Cost(0, 0), Seq.empty)
            case Some(StoreGroupNotExist(storeGroup)) => StoreGroupNotExistResult(storeGroup)
            case Some(e) =>
              throw new Exception("Unexpected error from data-coordinator on collocation status: " + e)
          }
        }
      }
    }
  }

  override def resync(dataset: DatasetInternalName, secondaryId: SecondaryId): Result = {
    withConnectRetries() {
      withHost(dataset, mutate = false) { host =>
        val request =  new BodylessHttpRequest(resyncReq(host, dataset, secondaryId).method("PUT"))
        httpClient.execute(request).run { r: Response =>
          errorFrom(r) match {
            case None => ResyncResult(secondaryId)
            case Some(DatasetNotInSecondary(secondaryId)) => DatasetNotInSecondaryResult(SecondaryId(secondaryId))
          }
        }
      }
    }
  }

  /**
   * EntityTag Seq from response object
   * @param r
   * @return
   */
  def etagsSeq(r: Response): Seq[EntityTag] = {
    r.headers("etag").map(EntityTagParser.parse(_ : String))
  }
}
