package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.{JBoolean, JObject}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.clients.geospace.GeospaceClient
import com.socrata.soda.clients.geospace.GeospaceClient._
import com.rojoma.json.v3.codec.JsonEncode

case class HealthZ(geospace: GeospaceClient) {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  object service extends SodaResource {
    override def get = { req => response =>
      val (code, fields) = geospace.versionCheck match {
        case Success =>
          (OK, Map("geospace_ok" -> JBoolean.canonicalTrue))
        case f: Failure =>
          val details = JsonEncode.toJValue(f)
          (InternalServerError, Map("geospace_ok" -> JBoolean.canonicalFalse, "details" -> details))
      }

      (code ~> Json(JObject(fields)))(response)
    }
  }
}
