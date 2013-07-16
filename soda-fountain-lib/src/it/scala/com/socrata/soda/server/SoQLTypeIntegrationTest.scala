package com.socrata.soda.server

import org.scalatest.{BeforeAndAfterAll, Suite}
import com.rojoma.json.ast._

trait SoQLTypeIntegrationTestFixture extends BeforeAndAfterAll with IntegrationTestHelpers { this: Suite =>

  //TODO: when these tests are stable, the rn can be refactored to be stable, and the fixture can simply truncate and replace rows, to reduce dataset churn.
  val resourceName = "soql-type-integration-test" + System.currentTimeMillis.toString

  override def beforeAll = {

    val cBody = JObject(Map(
      "resource_name" -> JString(resourceName),
      "name" -> JString("soda fountain soql type integration test"),
      "row_identifier" -> JArray(Seq(JString("test_id"))),
      "columns" -> JArray(Seq(
        column("the double type column",  "test_double",                    None, "double"              ),
        column("the money type column",   "test_money",                     None, "money"               ),
        column("a text column",           "test_text",                      None, "text"                ),
        column("fixed_typestamp column",  "test_fixed_timestamp",           None, "fixed_timestamp"     ),
        column("date column",             "test_date",                      None, "date"                ),
        column("time column",             "test_time",                      None, "time"                ),
        column("object column",           "test_object",                    None, "object"              ),
        column("array column",            "test_array",                     None, "array"               ),
        column("location column",         "test_location",                  None, "location"            ),
        //column("json column",             "test_json",                      None, "json"                ),
        column("a boolean column",        "test_boolean",                   None, "boolean"             ),
        column("floating_timestamp col",  "test_floating_timestamp",        None, "floating_timestamp"  ),
        column("the ID column",           "test_id",                        None, "number"              )
      ))
    ))
    val cResponse = dispatch("POST", "dataset", None, None, None,  Some(cBody))

    //publish
    val pResponse = dispatch("PUT", "dataset-copy", Some(resourceName), None, None, None)
  }
}

class SoQLTypeIntegrationTest extends IntegrationTest with SoQLTypeIntegrationTestFixture  {

  def testType( row: Map[String, JValue], query: String, expectedResult: String) = {
    val v = getVersionInSecondaryStore(resourceName)
    val uBody = JArray(Seq( JObject(row) ))
    val uResponse = dispatch("POST", "resource", Some(resourceName), None, None,  Some(uBody))
    assert(uResponse.getStatusCode == 200, uResponse.getResponseBody)

    waitForSecondaryStoreUpdate(resourceName, v)
    val params = Map(("$query" -> query))
    val qResponse = dispatch("GET", "resource", Some(resourceName), None, Some(params),  None)
    jsonCompare(qResponse.getResponseBody, expectedResult )
    qResponse.getStatusCode must equal (200)
  }

  test("upsert/query type number  ") { testType(Map(("test_id"->JNumber(100))),"select * where test_id = 100",  """[{test_id:100.0}]""".stripMargin)}
  test("upsert/query type double  ") { testType(Map(("test_id"->JNumber(101)), ("test_double" -> JNumber(0.333))),  "select * where test_double = 0.333",  """[{test_double:0.333,  test_id:101 }]""".stripMargin) }
  test("upsert/query type money   ") { testType(Map(("test_id"->JNumber(102)), ("test_money" -> JNumber(1.59))),  "select * where test_money = 1.59",  """[{test_money: 1.59,  test_id:102 }]""".stripMargin) }
  test("upsert/query type text    ") { testType(Map(("test_id"->JNumber(103)), ("test_text" -> JString("eastlake"))),  "select * where test_text = 'eastlake'",  """[{test_text: 'eastlake',  test_id:103 }]""".stripMargin) }
  test("upsert/query type object  ") { testType(Map(("test_id"->JNumber(107)), ("test_object" -> JObject(Map(("firstname"->JString("daniel")),("lastname"-> JString("rathbone")))))),  "select * where test_object.firstname = 'daniel'",  """[{test_object:{firstname:'daniel', lastname:'rathbone'},  test_id:107 }]""".stripMargin) }
  test("upsert/query type array   ") { testType(Map(("test_id"->JNumber(108)), ("test_array" -> JArray(Seq(JBoolean(true), JNumber(99))))),  "select * where test_array[0] = true",  """[{test_array :[true, 99.0],  test_id:108 }]""".stripMargin) }
  test("upsert/query type location") { testType(Map(("test_id"->JNumber(109)), ("test_location" -> JArray(Seq(JNumber(45.0), JNumber(39.0), JNull)))),  "select * where test_location.latitude = 45.0",  """[{test_location:[45.0, 39.0, null],  test_id:109 }]""".stripMargin) }
  //test("upsert/query type json    ") { testType(Map(("test_id"->JNumber(110)), ("test_json    " -> )),  "select * where test_json    = ",  """[{test_json    :,  test_id:110 }]""".stripMargin) }
  test("upsert/query type boolean ") { testType(Map(("test_id"->JNumber(111)), ("test_boolean" -> JBoolean(true))),  "select * where test_boolean = true",  """[{test_boolean:true,  test_id:111 }]""".stripMargin) }
  test("upsert/query type date    ") { testType(Map(("test_id"->JNumber(105)), ("test_date" -> JString("2013-07-15"))),  "select * where test_date    = '2013-07-15'",  """[{test_date    :'2013-07-15',  test_id:105 }]""".stripMargin) }
  test("upsert/query type time    ") { testType(Map(("test_id"->JNumber(106)), ("test_time" -> JString("02:10:49.123"))),  "select * where test_time    = '02:10:49.123'",  """[{test_time    :'02:10:49.123',  test_id:106 }]""".stripMargin) }

  test("upsert/query type fixed_timestamp   ") {
    testType(
      Map(("test_id"->JNumber(104)), ("test_fixed_timestamp" -> JString("2013-07-15T02:10:49.123Z"))),
      "select * where test_fixed_timestamp = '2013-07-15T02:10:49.123Z'",
      """[{test_fixed_timestamp :'2013-07-15T02:10:49.123Z',  test_id:104 }]""".stripMargin
    )
  }

  test("upsert/query type floating_timestamp") {
    testType(
      Map(("test_id"->JNumber(112)), ("test_floating_timestamp" -> JString("2013-07-15T02:10:49.123"))),
      "select * where test_floating_timestamp = '2013-07-15T02:10:49.123'",
      """[{test_floating_timestamp:'2013-07-15T02:10:49.123',  test_id:112 }]""".stripMargin
    )
  }
}
