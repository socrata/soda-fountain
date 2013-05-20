package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client._
import com.rojoma.json.ast._
import com.socrata.soda.server.services.ClientRequestExtractor._
import com.rojoma.json.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.json.io.{JsonBadParse, JsonReader}
import scala.collection.Map
import scala.concurrent.ExecutionContext.Implicits.global


trait DatasetService extends SodaService {

  object dataset {

    def upsert(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val it = streamJsonArrayValues(request, 1000000L)  //TODO: read this limit from config
      it match {
        case Right(boundedIt) => {
          try {
            for {
              datasetId <- store.translateResourceName(resourceName)
              schema <- dc.getSchema(datasetId)
            } {
              val upserts = boundedIt.map { rowjval =>
                rowjval match {
                  case JObject(map) =>  UpsertRow(map)
                  case _ => throw new Error("unexpected value")
                }
              }
              val r = dc.update(datasetId, None, mockUser, upserts)
              r() match {
                case Right(rr) => OK
                case Left(thr) => sendErrorResponse(thr.getMessage, "upsert.error", InternalServerError, None)
              }
            }
            sendErrorResponse("could not find datasetId or schema", "dataset.not-found", NotFound, None)
          }
          catch {
            case bp: JsonBadParse => sendErrorResponse("bad JSON value: " + bp.getMessage, "json.value.invalid", BadRequest, None)
          }
        }
        case Left(err) => sendErrorResponse("Error upserting values", err, BadRequest, None)
      }
    }
    def replace(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def truncate(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def query(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
    }
    def create()(request:HttpServletRequest): HttpServletResponse => Unit =  {
      DatasetSpec(request.getReader) match {
        case Right(dspec) => {
          val columnInstructions = dspec.columns.map(c => new AddColumnInstruction(c.fieldName, c.dataType))
          val instructions = dspec.rowId match{
            case Some(rid) => columnInstructions :+ SetRowIdColumnInstruction(rid)
            case None => columnInstructions
          }
          val r = dc.create(dspec.resourceName, mockUser, Some(instructions.iterator), dspec.locale )
          r() match {
            case Right((datasetId, records)) => {
              store.store(dspec.resourceName, datasetId)  // TODO: handle failure here, see list of errors from DC.
              OK
            }
            case Left(thr) => sendErrorResponse("could not create dataset", "internal.error", InternalServerError, Some(JString(thr.getMessage)))
          }
        }
        case Left(ers: Seq[String]) => sendErrorResponse( ers.mkString(" "), "dataset.specification.invalid", BadRequest, None )
      }
    }
    def setSchema(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      ColumnSpec.array(request.getReader) match {
        case Right(specs) => ???
        case Left(ers) => sendErrorResponse( ers.mkString(" "), "column.specification.invalid", BadRequest, None )
      }
    }
    def getSchema(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      val ido = store.translateResourceName(resourceName)
      ido match {
        case Some(id) => {
          val f = dc.getSchema(id)
          val r = f()
          r match {
            case Right(schema) => OK ~> Content(schema.toString)
            case Left(err) => sendErrorResponse(err, "dataset.schema.notfound", NotFound, Some(JString(resourceName)))
          }
        }
        case None => sendErrorResponse("resource name not recognized", "resourceName.invalid", NotFound, Some(JString(resourceName)))
      }
    }
    def delete(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("delete request not implemented")
    }


  }
}
