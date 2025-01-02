package com.socrata.soda.server.highlevel

import scala.collection.{mutable => scm}
import com.socrata.http.server.util.RequestId.RequestId
import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.server.highlevel.DatasetDAO.CannotAcquireDatasetWriteLock
import com.socrata.soda.server.highlevel.DatasetDAO.FeedbackInProgress
import com.socrata.soda.server.id._
import com.socrata.soda.server.persistence.{DatasetRecordLike, NameAndSchemaStore}
import com.socrata.soda.server.wiremodels._
import com.socrata.soda.server.SodaUtils.traceHeaders
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{Compound, Leaf}
import com.socrata.soql.brita.IdentifierFilter
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import DatasetDAO._

import scala.util.control.ControlThrowable
import com.socrata.soda.server.copy.{Discarded, Published, Stage, Unpublished}
import com.socrata.soda.server.resources.{DCCollocateOperation, NewRollup, SFCollocateOperation}
import com.socrata.soda.server.util.RelationSide.{From, RelationSide, To}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.parsing.RecursiveDescentParser.ParseException
import com.socrata.soql.parsing.standalone_exceptions.BadParse
import com.socrata.soql.analyzer2.{DatabaseColumnName, DatabaseTableName, UnparsedFoundTables}
import org.joda.time.DateTime
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.resource_groups.client.ResourceGroupsClient

class DatasetDAOImpl(dc: DataCoordinatorClient,
                     fbm: FeedbackSecondaryManifestClient,
                     rg: ResourceGroupsClient,
                     store: NameAndSchemaStore,
                     columnSpecUtils: ColumnSpecUtils,
                     instanceForCreate: () => String) extends DatasetDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetDAOImpl])
  val defaultDescription = ""
  val defaultPrimaryKey = ColumnName(":id")
  val defaultLocale = "en_US"

  def validResourceName(rn: ResourceName) = IdentifierFilter(rn.name) == rn.name

  // We only support updating the resource name right now. The rest is left as a TODO.
  def freezeForUpdate(spec: UserProvidedDatasetSpec): Either[Result, ResourceName] = spec match {
    case UserProvidedDatasetSpec(Some(resourceName), None, None, None, None, None) =>
      Right(resourceName)
    case other =>
      log.warn("Soda Fountain does not yet support the following patch request: " + spec)
      Left(UnsupportedUpdateOperation("Soda Fountain does not yet support the following patch request: " + spec))
  }

  def freezeForCreation(spec: UserProvidedDatasetSpec): Either[Result, DatasetSpec] = {
    spec match {
      case UserProvidedDatasetSpec(Some(resourceName), Some(name), description, rowIdentifier, locale, columns) =>
        val trueDesc = description.getOrElse(defaultDescription)
        val trueRID = rowIdentifier.getOrElse(defaultPrimaryKey)
        val trueLocale = locale.flatten.getOrElse(defaultLocale)
        val trueColumns = columns.getOrElse(Seq.empty).foldLeft(Map.empty[ColumnName, ColumnSpec]) { (acc, userColumnSpec) =>
          columnSpecUtils.freezeForCreation(acc.mapValues { col => (col.id, col.datatype) }, userColumnSpec) match {
            case ColumnSpecUtils.Success(cSpec) => acc + (cSpec.fieldName -> cSpec)
            // TODO: not-success case
            // TODO other cases have not been implemented
            case x =>
              log.warn("case is NOT implemented: %s".format(x.toString))
              ???
          }
        }
        Right(DatasetSpec(resourceName, name, trueDesc, trueRID, trueLocale, None, trueColumns))
      // TODO: Not-success case
    }
  }

  def createDataset(user: String, spec: UserProvidedDatasetSpec): Result =
    freezeForCreation(spec) match {
      case Right(frozenSpec) => createDataset(user, frozenSpec)
      case Left(result) => result
    }

  def createDataset(user: String, spec: DatasetSpec): Result = {
    if(!validResourceName(spec.resourceName)) return InvalidDatasetName(spec.resourceName)
    store.translateResourceName(spec.resourceName) match {
      case None =>
        // sid col is a little painful; if it's ":id" we want to do NOTHING.
        // If it's a system column OTHER THAN :id we want to add the set-row-id instruction but NOT look it up in the spec
        // If it's anything else we want to ensure it's in the spec AND issue the set-row-id-instruction
        val ridFieldName = spec.rowIdentifier
        val addRidInstruction =
          if(ridFieldName == defaultPrimaryKey) {
            Nil
          } else if(columnSpecUtils.systemColumns.contains(ridFieldName)) {
            List(new SetRowIdColumnInstruction(columnSpecUtils.systemColumns(ridFieldName).id))
          } else {
            if(!spec.columns.contains(ridFieldName)) return NonExistentColumn(spec.resourceName, ridFieldName)
            List(new SetRowIdColumnInstruction(spec.columns(ridFieldName).id))
          }
        // ok cool.  First send it upstream, then if that works stick it in the store.
        val columnInstructions = spec.columns.values.map { c => new AddColumnInstruction(c.datatype, c.fieldName, Some(c.id), c.computationStrategy) }

        val instructions = columnInstructions ++ addRidInstruction

        val (reportMetaData, _) = dc.create(spec.resourceName,
                                            instanceForCreate(),
                                            user,
                                            Some(instructions.iterator),
                                            spec.locale)

        val trueSpec = spec.copy(columns = spec.columns ++ columnSpecUtils.systemColumns)
        val record = trueSpec.asRecord(reportMetaData)

        // sanity-check
        locally {
          val dcSchema = dc.getSchema(record.handle)
          val mySchema = record.schemaSpec
          assert(dcSchema == Some(mySchema), "Schema spec differs between DC and me!:\n" + dcSchema + "\n" + mySchema)
        }

        store.addResource(record)
        store.updateVersionInfo(reportMetaData.datasetId, reportMetaData.version, reportMetaData.lastModified, None, Stage.InitialCopyNumber)

        val strategyTypes = spec.columns.flatMap { case (_, colSpec) => colSpec.computationStrategy }.map { _.strategyType }.toSet
        fbm.maybeReplicate(record.handle, strategyTypes)

        Created(trueSpec.asRecord(reportMetaData))
      case Some(_) =>
        DatasetAlreadyExists(spec.resourceName)
    }
  }

  def replaceOrCreateDataset(user: String,
                             dataset: ResourceName,
                             spec: UserProvidedDatasetSpec): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        freezeForCreation(spec) match {
          case Right(frozenSpec) =>
            if(datasetRecord.locale != frozenSpec.locale) return LocaleChanged(frozenSpec.locale)
            // ok.  What we need to do now is turn this into a list of operations to hand
            // off to the data coordinator and/or our database.  This will include:
            //  1. getting the current schema from the D.C. or cache
            //  2. Figuring out what columns need to be renamed (note: this may include cycles!) and/or added or deleted.
            //  3. Issuing column updates (add/delete) to the D.C.  This may fail with a "schema out of date"
            //     error; if so go back to step 1.
            //  4. update our representation of the schema (i.e., rename columns) and save name, description changes.
            //
            // TODO: Figure out what happens in the event of inconsistency!!!
            ???
          case Left(result) =>
            result
        }
      case None =>
        DatasetNotFound(dataset)
    }

  def updateDataset(user: String,
                    dataset: ResourceName,
                    spec: UserProvidedDatasetSpec): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        freezeForUpdate(spec) match {
          case Right(resourceName) =>
            // Ensure the new resource name is valid
            if(!validResourceName(resourceName))
              return InvalidDatasetName(resourceName)

            // Ensure the new resource name doesn't collide with another dataset
            store.translateResourceName(resourceName) match {
              case Some(existing) =>
                DatasetAlreadyExists(resourceName)
              case None =>
                // Make the changes
                store.patchResource(datasetRecord.resourceName, resourceName)

                // Retrieve the updated record to send back in the response
                store.lookupDataset(resourceName).headOption match {
                  case Some(updated) =>
                    Updated(updated)
                  // The below should never happen... #lastwords
                  case None =>
                    throw new Exception("Could not find renamed dataset in metasoda")
                }
            }
          case Left(result) => result
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def removeDataset (user: String, dataset: ResourceName, expectedDataVersion: Option[Long]): Result = {
    retryable(limit = 5) {
      store.translateResourceName(dataset, deleted = true) match {
        case Some(datasetRecord) =>
          dc.deleteAllCopies(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, "")
          {
            case DataCoordinatorClient.NonCreateScriptResult(_, _, _, _, _, _) =>
              store.removeResource(dataset)
              Deleted
            case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
              // should only have two error case for this path.
            case DataCoordinatorClient.DatasetNotFoundResult(_) =>
              // if it isn't known to dc, consider it gone
              log.warn(s"Dataset $dataset / ${datasetRecord.systemId} not found in data-coordinator, removing from soda-fountain")
              store.removeResource(dataset)
              Deleted
            case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
              CannotAcquireDatasetWriteLock(dataset)
            case DataCoordinatorClient.DatasetVersionMismatchResult(_, v) =>
              DatasetDAO.DatasetVersionMismatch(datasetRecord.resourceName, v)
            case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
              InternalServerError(code, tag, data)
            case x =>
              log.warn("case is NOT implemented %s".format(x.toString))
              InternalServerError("unknown", tag, x.toString)
          }
        case None =>
          DatasetNotFound(dataset)
      }
    }
  }

  def markDatasetForDeletion(user: String, dataset: ResourceName, datetime: Option[DateTime]): Result = {
    //TODO: Ask about retryable and give a return value
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          store.markResourceForDeletion(dataset, datetime)
          try{
            rg.deregisterDataset(dataset.toString())
          }catch{
            case e:Exception=> log.error("Failed to deregister from resource groups",e)
          }
          Deleted
        case None =>
          DatasetNotFound(dataset)
      }
  }

  def unmarkDatasetForDeletion(user: String, dataset: ResourceName) : Result = {
    store.translateResourceName(dataset, None, true) match {
      case Some(datasetRecord) =>
        store.unmarkResourceForDeletion(dataset)
        Undeleted
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def getSecondaryVersions(dataset: ResourceName): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.checkVersionInSecondaries(datasetRecord.handle) match {
          case Right(vrs) => vrs.map(DatasetSecondaryVersions).getOrElse(DatasetNotFound(dataset))
          case Left(fail) => UnexpectedInternalServerResponse(fail.reason, fail.tag)
        }
      case None =>
        DatasetNotFound(dataset)
    }

  def getVersion(dataset: ResourceName, secondary: SecondaryId): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.checkVersionInSecondary(datasetRecord.handle, secondary) match {
          case Right(vr) => vr.map(DatasetVersion).getOrElse(DatasetNotFound(dataset))
          case Left(fail) => UnexpectedInternalServerResponse(fail.reason, fail.tag)
        }
      case None =>
        DatasetNotFound(dataset)
    }

  def getCurrentCopyNum(dataset: ResourceName): Option[Long] =
    store.lookupCopyNumber(dataset, Some(Published)) match {
      case None => store.lookupCopyNumber(dataset, Some(Unpublished))
      case o: Some[Long] => o
    }

  def getDataset(dataset: ResourceName, stage: Option[Stage]): Result =
    store.lookupDataset(dataset, stage) match {
      case Some(datasetRecord) =>
        Found(datasetRecord)
      case None =>
        DatasetNotFound(dataset)
    }

  class Retry extends ControlThrowable

  def retryable[T](limit: Int /* does not include the initial try */)(f: => T): T = {
    var count = 0
    var done = false
    var result: T = null.asInstanceOf[T]
    do {
      try {
        result = f
        done = true
      } catch {
        case _: Retry =>
          count += 1
          if(count > limit) throw new Exception("Retried too many times")
      }
    } while(!done)
    result
  }
  def retry() = throw new Retry

  def makeCopy(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], copyData: Boolean): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.copy(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, copyData, user) {
            case DataCoordinatorClient.NonCreateScriptResult(_, _, newCopyNumber, newVersion, newShapeVersion, lastModified) =>
              store.makeCopy(datasetRecord.systemId, newCopyNumber, newVersion)
              WorkingCopyCreated(newVersion, newShapeVersion)
            case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
            case DataCoordinatorClient.DatasetNotFoundResult(_) =>
              DatasetNotFound(dataset)
            case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
              CannotAcquireDatasetWriteLock(dataset)
            case DataCoordinatorClient.FeedbackInProgressResult(_, _, stores) =>
              FeedbackInProgress(dataset, stores)
            case DataCoordinatorClient.IncorrectLifecycleStageResult(actualStage: String, expectedStage: Set[String]) =>
              IncorrectLifecycleStageResult(actualStage, expectedStage)
            case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
              InternalServerError(code, tag, data)
            case x =>
              log.warn("case is NOT implemented %s".format(x.toString))
              InternalServerError("unknown", tag, x.toString)
          }
        case None =>
          DatasetNotFound(dataset)
      }
    }

  def dropCurrentWorkingCopy(user: String, dataset: ResourceName, expectedDataVersion: Option[Long]): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          // Cannot use copy number from DC because it indicates the latest surviving copy.
          store.lookupCopyNumber(dataset, Some(Unpublished)) match {
            case Some(unpublishCopyNumber) =>
              dc.dropCopy(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user) {
                case DataCoordinatorClient.NonCreateScriptResult(_, _, _, newVersion, newShapeVersion, lastModified) =>
                  store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, Some(Discarded), unpublishCopyNumber)
                  WorkingCopyDropped(newVersion, newShapeVersion)
                case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
                  store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
                  retry()
                case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                  DatasetNotFound(dataset)
                case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
                  CannotAcquireDatasetWriteLock(dataset)
                case DataCoordinatorClient.FeedbackInProgressResult(_, _, stores) =>
                  FeedbackInProgress(dataset, stores)
                case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
                  InternalServerError(code, tag, data)
                case x =>
                  log.warn("case is NOT implemented %s".format(x.toString))
                  InternalServerError("unknown", tag, x.toString)
              }
            case None =>
              DatasetNotFound(dataset)
          }
        case None =>
          DatasetNotFound(dataset)
      }
    }

  def publish(user: String, dataset: ResourceName, expectedDataVersion: Option[Long]): Result =
    retryable(limit = 5) {
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          dc.publish(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user) {
            case DataCoordinatorClient.NonCreateScriptResult(_, _, copyNumber, newVersion, newShapeVersion, lastModified) =>
              store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, Some(Published), copyNumber)
              WorkingCopyPublished(newVersion, newShapeVersion)
            case DataCoordinatorClient.SchemaOutOfDateResult(newSchema) =>
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              retry()
            case DataCoordinatorClient.DatasetNotFoundResult(_) =>
              DatasetNotFound(dataset)
            case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
              CannotAcquireDatasetWriteLock(dataset)
            case DataCoordinatorClient.FeedbackInProgressResult(_, _, stores) =>
              FeedbackInProgress(dataset, stores)
            case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
              InternalServerError(code, tag, data)
            case x =>
              log.warn("case is NOT implemented %s".format(x.toString))
              InternalServerError("unknown", tag, x.toString)
          }
        case None =>
          DatasetNotFound(dataset)
      }
    }

  def propagateToSecondary(dataset: ResourceName, secondary: SecondaryId, secondariesLike: Option[ResourceName]): Result = {

    val secondariesLikeDatasetId = secondariesLike.flatMap(rn => store.translateResourceName(rn).map(_.systemId))
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.propagateToSecondary(datasetRecord.handle, secondary, secondariesLikeDatasetId)
        PropagatedToSecondary
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def deleteFromSecondary(dataset: ResourceName, secondary: SecondaryId): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.deleteFromSecondary(datasetRecord.handle, secondary)
        DeletedFromSecondary
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def replaceOrCreateRollup(user: String,
                            dataset: ResourceName,
                            expectedDataVersion: Option[Long],
                            rollup: RollupName,
                            spec: UserProvidedRollupSpec): Result =
      store.translateResourceName(dataset) match {
        case Some(datasetRecord) =>
          spec.soql match {
            case Some(soql) =>
              log.debug(s"soql for rollup ${rollup} is: ${soql}")
              try {
                val (parsedQueries, tableNames) = RollupHelper.parse(soql)
                RollupHelper.validate(store, dataset, parsedQueries, tableNames)
                val mappedQueries = RollupHelper.mapQuery(store, dataset, parsedQueries, tableNames, generateAliases = true)
                log.debug(s"Mapped soql for rollup ${rollup} is: ${mappedQueries}")
                val instruction = CreateOrUpdateRollupInstruction(rollup, mappedQueries.toString(), soql)
                dc.update(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user,
                  Iterator.single(instruction)) {
                  case DataCoordinatorClient.NonCreateScriptResult(report, etag, copyNumber, newVersion, newShapeVersion, lastModified) =>
                    store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber)
                    RollupHelper.rollupCreatedOrUpdated(store,dataset,copyNumber,rollup,mappedQueries, tableNames)
                    RollupCreatedOrUpdated
                  case DataCoordinatorClient.NoSuchRollupResult(_, _) =>
                    RollupNotFound(rollup)
                  case DataCoordinatorClient.InvalidRollupResult(ru, _) =>
                    RollupError(ru.name)
                  case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                    DatasetNotFound(dataset)
                  case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
                    CannotAcquireDatasetWriteLock(dataset)
                  case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
                    InternalServerError(code, tag, data)
                  case x =>
                    log.warn("case is NOT implemented %s".format(x.toString))
                    InternalServerError("unknown", tag, x.toString)
                }
              } catch {
                case ex: ParseException =>
                  RollupError(s"soql parse error ${ex.getMessage}" )
                case ex: SoQLException =>
                  RollupError(s"soql analyze error ${ex.getMessage}" )
                case ex: RollupException =>
                  RollupError(s"rollup error: ${ex.getMessage}" )
              }
            case None =>
              RollupError("soql field missing")
          }
        case None =>
          DatasetNotFound(dataset)
      }

  private def dsContext(ds: DatasetRecordLike): DatasetContext[SoQLType] = {
    val columnIdMap: Map[ColumnName, String] = ds.columnsByName.mapValues(_.id.underlying)
    val rawSchema: Map[String, SoQLType] = ds.schemaSpec.schema.map { case (k, v) => (k.underlying, v) }

    new DatasetContext[SoQLType] {
      val schema = OrderedMap(columnIdMap.mapValues(rawSchema).toSeq.sortBy(_._1) : _*)
    }
  }
  def getRollupRelations(dataset: ResourceName, relationSide: RelationSide): Result ={
    (relationSide match{
      case From => store.rollupDatasetRelationBySecondaryDataset _
      case To => store.rollupDatasetRelationByPrimaryDataset _
    })(dataset,store.latestCopyNumber(dataset)) match {
      case i: Set[RollupDatasetRelation] if i.isEmpty => RollupRelationsNotFound()
      case rollupDatasetRelations=> RollupRelations(rollupDatasetRelations.map{relation=>
        val (parsedQueriesInternal, tableNamesInternal) = RollupHelper.parse(relation.soql)
        val soqlUser = RollupHelper.reverseMapQuery(store, relation.primaryDataset, parsedQueriesInternal, tableNamesInternal)
        relation.copy(soql = soqlUser)
      })
    }
  }

  override def markRollupAccessed(resourceName: ResourceName,rollupName: RollupName):Result={
    if (store.markRollupAccessed(resourceName,store.latestCopyNumber(resourceName),rollupName)){
      RollupMarkedAccessed()
    }else{
      RollupNotFound(rollupName)
    }
  }

  private def convertNewAnalyzerDCRollup(datasetRecord: DatasetRecordLike, rollup: RollupInfo): RollupSpec = {
    JsonUtil.parseJson[NewRollup.NewRollupSoql](rollup.soql) match {
      case Right(newRollup) =>
        val cache = new scm.HashMap[DataCoordinatorClient.RollupMetaTypes.TableName, DatasetRecordLike]
        cache += DataCoordinatorClient.RollupMetaTypes.TableName.ResourceName(datasetRecord.resourceName) -> datasetRecord
        cache += DataCoordinatorClient.RollupMetaTypes.TableName.InternalName(datasetRecord.systemId) -> datasetRecord

        def lookupRecord(resourceName: DataCoordinatorClient.RollupMetaTypes.TableName): Option[DatasetRecordLike] =
          cache.get(resourceName).orElse {
            val lookupResult = resourceName match {
              case DataCoordinatorClient.RollupMetaTypes.TableName.InternalName(in) =>
                store.lookupDataset(in, Some(Published))
              case DataCoordinatorClient.RollupMetaTypes.TableName.ResourceName(rn) =>
                store.lookupDataset(rn, Some(Published))
            }
            lookupResult match {
              case Some(record) =>
                cache += DataCoordinatorClient.RollupMetaTypes.TableName.ResourceName(record.resourceName) -> record
                cache += DataCoordinatorClient.RollupMetaTypes.TableName.InternalName(record.systemId) -> record
                Some(record)
              case None =>
                None
            }
          }

        // A new-rollup coming back from DC will be in the
        // DataCoordinatorClient.UnstagedMetaTypes space; we need to
        // convert that back to our native RollupMetaTypes space.
        //
        // We also might want to tolerate schema changes better (cf
        // the FoundTablesTableFinder thing that the RollupManager
        // does over in soql-pg)
        val convertedFoundTables =
          newRollup.foundTables.rewriteDatabaseNames[metatypes.RollupMetaTypes](
            {
              case dtn@DatabaseTableName(name) =>
                // just make sure the cache gets populated
                lookupRecord(name) match {
                  case Some(record) =>
                    DatabaseTableName(record.resourceName)
                  case None =>
                    log.warn(s"invalid new-analyzer rollup ${rollup.name}: No record of resource name ${name}")
                    return RollupSpec(name = rollup.name, soql = "__Invalid SoQL__")
                }
            },
            { case (DatabaseTableName(resourceName), DatabaseColumnName(columnId)) =>
              val record = lookupRecord(resourceName).getOrElse {
                log.warn(s"invalid new-analyzer rollup ${rollup.name}: No record of resource name ${resourceName}")
                return RollupSpec(name = rollup.name, soql = "__Invalid SoQL__")
              }
              record.columnsById.get(columnId) match {
                case Some(col) =>
                  DatabaseColumnName(col.fieldName)
                case None =>
                  log.warn(s"invalid new-analyzer rollup ${rollup.name}: No record of column id ${columnId}")
                  return RollupSpec(name = rollup.name, soql = "__Invalid SoQL__")
              }
            }
          )

        val convertedRollup =
          NewRollupRequest.Stored(
            convertedFoundTables,
            newRollup.userParameters,
            newRollup.rewritePasses
          )

        RollupSpec(
          name = rollup.name,
          soql = JsonUtil.renderJson(convertedRollup, pretty = false)
        )
      case Left(err) =>
        log.warn(s"invalid new-analyzer rollup ${rollup.name}: ${err.english}")
        RollupSpec(name = rollup.name, soql = "__Invalid SoQL__")
    }
  }

  def getRollups(dataset: ResourceName): Result = {
    store.lookupDataset(dataset, store.latestCopyNumber(dataset)) match {
      case Some(datasetRecord) =>
        //Ideally I'd like to no longer reach out to DC after starting to track rollup metadata in soda fountain.
        //However, the DC has historical knowledge of rollups that predates Soda knowledge.
        //Soda would only know about rollup metadata that was created since the Soda enhancement.
        //We could synchronize DC rollup knowledge to Soda, but that needs to have some additional thought.
        //So lets do both for now.
        dc.getRollups(datasetRecord.handle) match {
          case result: DataCoordinatorClient.RollupResult =>
            val dcRollups = result.rollups.map { rollup =>
              if(rollup.isNewAnalyzer) {
                convertNewAnalyzerDCRollup(datasetRecord, rollup)
              } else {
                try {
                  val (parsedQueries, tableNames) = RollupHelper.parse(rollup.soql)
                  val mappedQueries = RollupHelper.reverseMapQuery(store, dataset, parsedQueries, tableNames)
                  log.debug(s"soql for rollup ${rollup} is: ${parsedQueries}")
                  log.debug(s"Mapped soql for rollup ${rollup} is: ${mappedQueries}")
                  RollupSpec(name = rollup.name, soql = mappedQueries.toString())
                } catch {
                  case ex: BadParse =>
                    log.warn(s"invalid rollup SoQL ${rollup.name} ${rollup.soql} ${ex.getMessage}")
                    RollupSpec(name = rollup.name, soql = "__Invalid SoQL__")
                  case ex: Exception =>
                    log.warn(s"invalid rollup SoQL ${rollup.name} ${rollup.soql} ${ex.getMessage}")
                    RollupSpec(name = rollup.name, soql = "__Invalid Rollup__")
                }
              }
            }

            val sodaRollups = store.getRollups(
              dataset,
              store.latestCopyNumber(dataset)
            ).toSeq.map { rollup =>
              if(rollup.isNewAnalyzer) {
                // New-rollups don't get reworked, so we can just hand back the soql unmodified
                RollupSpec(
                  name = rollup.name,
                  soql = rollup.soql,
                  lastAccessed = Some(rollup.lastAccessed)
                )
              } else {
                try {
                  val (parsedQueries, tableNames) = RollupHelper.parse(rollup.soql)
                  val soqlWithUserIdentifiers = RollupHelper.reverseMapQuery(store, dataset, parsedQueries, tableNames)
                  RollupSpec(
                    name = rollup.name,
                    soql = soqlWithUserIdentifiers,
                    //Soda knows the lastAccessed date of a rollup
                    lastAccessed = Some(rollup.lastAccessed))
                } catch {
                  case ex: BadParse =>
                    log.warn(s"invalid rollup SoQL ${rollup.name} ${rollup.soql} ${ex.getMessage}")
                    RollupSpec(name = rollup.name, soql = "__Invalid SoQL__")
                  case ex: Exception =>
                    log.warn(s"invalid rollup SoQL ${rollup.name} ${rollup.soql} ${ex.getMessage}")
                    RollupSpec(name = rollup.name, soql = "__Invalid Rollup__")
                }
              }
            }

            Rollups(
              //Convert both dc and soda rollup specs to a map of [spec.name,spec]
              //Merge them together
              sodaRollups.map(a => a.name -> a).toMap.foldLeft(dcRollups.map(a => a.name -> a).toMap){
                case (acc,(rollupName,rollupSpec))=>
                  acc.updated(rollupName,acc.get(rollupName).map{ dcRollupSpec=>
                    if (dcRollupSpec.soql!=rollupSpec.soql){
                      log.error(s"Rollup soql mismatch. DC:'${dcRollupSpec.soql}', Soda:'${rollupSpec.soql}'")
                      dcRollupSpec
                    }else{
                      rollupSpec
                    }
                  }.getOrElse(rollupSpec))
              }.values.toSeq
            )
          case DataCoordinatorClient.DatasetNotFoundResult(_) =>
            DatasetNotFound(dataset)
          case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
            InternalServerError(code, tag, data)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def deleteRollups(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], rollups: Seq[RollupName]): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        val instruction = rollups.map(DropRollupInstruction).toIterator
        dc.update(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user, instruction) {
          case DataCoordinatorClient.NonCreateScriptResult(report, etag, copyNumber, newVersion, newShapeVersion, lastModified) =>
            store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber)
            store.deleteRollups(dataset,copyNumber,rollups.toSet)
            RollupDropped
          case DataCoordinatorClient.NoSuchRollupResult(ru, _) =>
            RollupNotFound(ru)
          case DataCoordinatorClient.DatasetNotFoundResult(_) =>
            DatasetNotFound(dataset)
          case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
            CannotAcquireDatasetWriteLock(dataset)
          case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
            InternalServerError(code, tag, data)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def replaceOrCreateIndex(user: String,
                           dataset: ResourceName,
                           expectedDataVersion: Option[Long],
                           name: IndexName,
                           spec: UserProvidedIndexSpec): Result =
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        spec.expressions match {
          case Some(expressions) =>
            log.debug(s"expressions for index ${name} is: ${expressions}")
            try {
              val instruction = CreateOrUpdateIndexInstruction(name, expressions, spec.filter)
              dc.update(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user,
                Iterator.single(instruction)) {
                case DataCoordinatorClient.NonCreateScriptResult(report, etag, copyNumber, newVersion, newShapeVersion, lastModified) =>
                  store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber)
                  IndexCreatedOrUpdated
                case DataCoordinatorClient.NoSuchIndexResult(_, _) =>
                  IndexNotFound(name)
                case DataCoordinatorClient.InvalidIndexResult(name, _) =>
                  IndexError(name.name)
                case DataCoordinatorClient.DatasetNotFoundResult(_) =>
                  DatasetNotFound(dataset)
                case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
                  CannotAcquireDatasetWriteLock(dataset)
                case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
                  InternalServerError(code, tag, data)
                case x =>
                  log.warn("case is NOT implemented %s".format(x.toString))
                  InternalServerError("unknown", tag, x.toString)
              }
            } catch {
              case ex: ParseException =>
                IndexError(s"index parse error ${ex.getMessage}" )
            }
          case None =>
            IndexError("index field missing")
        }
      case None =>
        DatasetNotFound(dataset)
    }

  def getIndexes(dataset: ResourceName): Result = {
    store.lookupDataset(dataset, store.latestCopyNumber(dataset)) match {
      case Some(datasetRecord) =>
        dc.getIndexes(datasetRecord.handle) match {
          case result: DataCoordinatorClient.IndexResult =>
            val indexes = result.indexes.map { index =>
              IndexSpec(name = index.name, expressions = index.expressions, index.filter)
            }
            Indexes(indexes)
          case DataCoordinatorClient.DatasetNotFoundResult(_) =>
            DatasetNotFound(dataset)
          case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
            InternalServerError(code, tag, data)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def deleteIndexes(user: String, dataset: ResourceName, expectedDataVersion: Option[Long], indexes: Seq[IndexName]): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        val instruction = indexes.map(DropIndexInstruction).toIterator
        dc.update(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user, instruction) {
          case DataCoordinatorClient.NonCreateScriptResult(report, etag, copyNumber, newVersion, newShapeVersion, lastModified) =>
            store.updateVersionInfo(datasetRecord.systemId, newVersion, lastModified, None, copyNumber)
            IndexDropped
          case DataCoordinatorClient.NoSuchIndexResult(name, _) =>
            IndexNotFound(name)
          case DataCoordinatorClient.DatasetNotFoundResult(_) =>
            DatasetNotFound(dataset)
          case DataCoordinatorClient.CannotAcquireDatasetWriteLockResult(_) =>
            CannotAcquireDatasetWriteLock(dataset)
          case DataCoordinatorClient.InternalServerErrorResult(code, tag, data) =>
            InternalServerError(code, tag, data)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def collocate(secondaryId: SecondaryId, operation: SFCollocateOperation, explain: Boolean, jobId: String): Result = {
    def translate(resource: ResourceName): Option[DatasetInternalName] = store.translateResourceName(resource).map(_.systemId)
    DCCollocateOperation(operation, translate _) match {
      case Left(op) =>
        // TODO: Translate dc errors better, need to clarify what actually needs to be propogated
        dc.collocate(secondaryId, op, explain, jobId) match {
          case res: DataCoordinatorClient.CollocateResult =>
            CollocateDone(res, { datasetId => store.bulkDatasetLookup(Set(datasetId)).headOption })
          case DataCoordinatorClient.InstanceNotExistResult(e) => GenericCollocateError(e)
          case DataCoordinatorClient.StoreGroupNotExistResult(e) => GenericCollocateError(e)
          case DataCoordinatorClient.StoreDoesNotSupportCollocationResult(e) => GenericCollocateError(e)
          case DataCoordinatorClient.StoreNotExistResult(e) => GenericCollocateError(e)
          case DataCoordinatorClient.DatasetNotExistResult(e) => GenericCollocateError(e.underlying)
          case DataCoordinatorClient.CollocationLockTimeoutResult() => CollocationLockTimeoutError()
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case Right(err) => err
    }
  }

  def collocateStatus(dataset: ResourceName, secondaryId: SecondaryId, jobId: String): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.collocateStatus(datasetRecord.handle, secondaryId, jobId) match {
          case res: DataCoordinatorClient.CollocateResult =>
            CollocateDone(res, { datasetId => store.bulkDatasetLookup(Set(datasetId)).headOption })
          case DataCoordinatorClient.StoreGroupNotExistResult(e) => GenericCollocateError(e)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def deleteCollocate(dataset: ResourceName, secondaryId: SecondaryId, jobId: String): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.deleteCollocate(datasetRecord.handle, secondaryId, jobId) match {
          case res: DataCoordinatorClient.CollocateResult =>
            CollocateDone(res, { datasetId => store.bulkDatasetLookup(Set(datasetId)).headOption })
          case DataCoordinatorClient.StoreGroupNotExistResult(e) => GenericCollocateError(e)
          case x =>
            log.warn("case is NOT implemented %s".format(x.toString))
            InternalServerError("unknown", tag, x.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  def secondaryReindex(user: String, dataset: ResourceName, expectedDataVersion: Option[Long]): Result = {
    store.translateResourceName(dataset) match {
      case Some(datasetRecord) =>
        dc.update(datasetRecord.handle, datasetRecord.schemaHash, expectedDataVersion, user, Iterator(SecondaryReindexInstruction()))(result => result) match {
          case _: DataCoordinatorClient.SuccessResult =>
            EmptyResult
          case e: DataCoordinatorClient.FailResult =>
            InternalServerError("unknown", tag, e.toString)
        }
      case None =>
        DatasetNotFound(dataset)
    }
  }

  private def tag: String = {
    val uuid = java.util.UUID.randomUUID().toString
    log.info("internal error; tag = " + uuid)
    uuid
  }
}
