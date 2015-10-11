package com.socrata.soda.server.resources
import com.socrata.http.common.util.ContentNegotiation

import com.socrata.soda.server._
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.OptionallyTypedPathComponent
import com.socrata.http.server.util.{Precondition, EntityTag, RequestId}
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.errors._
import com.socrata.soda.server.export.Exporter
import com.socrata.soda.server.highlevel.ExportDAO.{ColumnInfo, CSchema}
import com.socrata.soda.server.highlevel.{ColumnSpecUtils, ExportDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soql.types.SoQLValue
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

case class Export(exportDAO: ExportDAO, etagObfuscator: ETagObfuscator) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Export])

  implicit val contentNegotiation = new ContentNegotiation(Exporter.exporters.map { exp => exp.mimeType -> exp.extension }, List("en-US"))

  val headerHashAlg = "SHA1"
  val headerHashLength = MessageDigest.getInstance(headerHashAlg).getDigestLength
  def headerHash(req: HttpRequest) = {
    val hash = MessageDigest.getInstance(headerHashAlg)
    import com.socrata.http.server.HttpRequest.HttpRequestApi
    val reqApi = new HttpRequestApi(req)

    hash.update(reqApi.queryStr.toString.getBytes(StandardCharsets.UTF_8))
    hash.update(255.toByte)
    for(field <- ContentNegotiation.headers) {
      hash.update(field.getBytes(StandardCharsets.UTF_8))
      hash.update(254.toByte)
      for(elem <- reqApi.headers(field)) {
        hash.update(elem.getBytes(StandardCharsets.UTF_8))
        hash.update(254.toByte)
      }
      hash.update(255.toByte)
    }
    hash.digest()
  }

  def export(resourceName: ResourceName, ext: Option[String])(req: HttpRequest)(resp: HttpServletResponse) {
    exportCopy(resourceName, "published", ext)(req)(resp)
  }

  def exportCopy(resourceName: ResourceName, copy: String, ext: Option[String])(req: HttpRequest)(resp: HttpServletResponse) {
    // Etags generated by this system are the obfuscation of the etag from upstream plus
    // the hash of the contents of the header fields naemd by ContentNegotiation.headers.
    // So, when we receive etags in an if-none-match from the client
    //   1. decrypt the tags
    //   2. extract our bit of the data
    //   3. hash our headers and compare, dropping the etag completely if the hash is different
    //   4. Passing the remaining (decrypted and hash-stripped) etags upstream.
    //
    // For if-match it's the same, only we KEEP the ones that match the hash (and if that eliminates
    // all of them, then we "expectation failed" before ever passing upward to the data-coordinator)
    val limit = Option(req.getParameter("limit")).map { limStr =>
      try {
        limStr.toLong
      } catch {
        case e: NumberFormatException =>
          SodaUtils.errorResponse(req, BadParameter("limit", limStr))(resp)
          return
      }
    }

    val offset = Option(req.getParameter("offset")).map { offStr =>
      try {
        offStr.toLong
      } catch {
        case e: NumberFormatException =>
          SodaUtils.errorResponse(req, BadParameter("offset", offStr))(resp)
          return
      }
    }

    val excludeSystemFields = Option(req.getParameter("exclude_system_fields")).map { paramStr =>
      try {
        paramStr.toBoolean
      } catch {
        case e: Exception =>
          SodaUtils.errorResponse(req, BadParameter("exclude_system_fields", paramStr))(resp)
          return
      }
    }.getOrElse(true)

    val ifModifiedSince = req.dateTimeHeader("If-Modified-Since")

    val sorted = Option(req.getParameter("sorted")).map {
      case "true" => true
      case "false" => false
      case other => return SodaUtils.errorResponse(req, BadParameter("sorted", other))(resp)
    }.getOrElse(true)

    val suffix = headerHash(req)
    val precondition = req.precondition.map(etagObfuscator.deobfuscate)
    def prepareTag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(suffix))
    precondition.filter(_.endsWith(suffix)) match {
      case Right(newPrecondition) =>
        val passOnPrecondition = newPrecondition.map(_.dropRight(suffix.length))
        req.negotiateContent match {
          case Some((mimeType, charset, language)) =>
            val exporter = Exporter.exportForMimeType(mimeType)
            exportDAO.export(resourceName,
                             exporter.validForSchema,
                             Seq.empty,
                             passOnPrecondition,
                             ifModifiedSince,
                             limit,
                             offset,
                             copy,
                             sorted = sorted,
                             requestId = RequestId.getFromRequest(req)) {
              case ExportDAO.Success(fullSchema, newTag, fullRows) =>
                resp.setStatus(HttpServletResponse.SC_OK)
                resp.setHeader("Vary", ContentNegotiation.headers.mkString(","))
                newTag.foreach { tag =>
                  ETag(prepareTag(tag))(resp)
                }
                // TODO: DC export always includes system columns
                // Because system columns always are always next to each other,
                // We can drop them if we do not want them.
                // When DC has the option to exclude system columns,
                // move the work downstream to avoid tempering the row array.
                val isSystemColumn = (ci: ColumnInfo) => ColumnSpecUtils.isSystemColumn(ci.fieldName)
                val sysColsSize =
                  if (!excludeSystemFields) 0
                  else fullSchema.schema.filter(col => ColumnSpecUtils.isSystemColumn(col.fieldName)).size
                val sysColsStart = fullSchema.schema.indexWhere(isSystemColumn(_))
                val (schema: CSchema, rows) =
                  if (sysColsSize == 0) (fullSchema, fullRows)
                  else (fullSchema.copy(schema = fullSchema.schema.seq.filterNot(isSystemColumn(_))),
                        fullRows.map(row => row.take(sysColsStart) ++ row.drop(sysColsStart + sysColsSize)))
                exporter.export(resp, charset, schema, rows)
              case ExportDAO.PreconditionFailed =>
                SodaUtils.errorResponse(req, EtagPreconditionFailed)(resp)
              case ExportDAO.NotModified(etags) =>
                SodaUtils.errorResponse(req, ResourceNotModified(etags.map(prepareTag), Some(ContentNegotiation.headers.mkString(","))))(resp)
              case ExportDAO.SchemaInvalidForMimeType => SodaUtils.errorResponse(req, SchemaInvalidForMimeType)(resp)
              case ExportDAO.NotFound(x) => SodaUtils.errorResponse(req, GeneralNotFoundError(x.toString()))(resp)
            }
          case None =>
            // TODO better error
            NotAcceptable(resp)
        }
      case Left(Precondition.FailedBecauseNoMatch) =>
        SodaUtils.errorResponse(req, EtagPreconditionFailed)(resp)
    }
  }

  case class publishedService(resourceAndExt: OptionallyTypedPathComponent[ResourceName]) extends SodaResource {
    override def get = export(resourceAndExt.value, resourceAndExt.extension.map(Exporter.canonicalizeExtension))
  }

  case class service(resource: ResourceName, copyAndExt: OptionallyTypedPathComponent[String]) extends SodaResource {
    override def get = exportCopy(resource, copyAndExt.value, copyAndExt.extension.map(Exporter.canonicalizeExtension))
  }

  def extensions(s: String) = Exporter.exporterExtensions.contains(Exporter.canonicalizeExtension(s))
}
