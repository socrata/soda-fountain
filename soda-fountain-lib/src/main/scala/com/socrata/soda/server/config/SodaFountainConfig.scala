package com.socrata.soda.server.config

import com.typesafe.config.Config
import com.socrata.thirdparty.curator.{CuratorConfig, DiscoveryConfig}
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class SodaFountainConfig(config: Config) extends ConfigClass(WithDefaultAddress(config), "com.socrata.soda-fountain") {
  val maxDatumSize = getInt("max-datum-size")
  val etagObfuscationKey = optionally(getString("etag-obfuscation-key"))

  val curator = getConfig("curator", new CuratorConfig(_, _))
  val discovery = getConfig("service-advertisement", new DiscoveryConfig(_, _))
  val network = getConfig("network", new NetworkConfig(_, _))
  val dataCoordinatorClient = getConfig("data-coordinator-client", new DataCoordinatorClientConfig(_, _))
  val queryCoordinatorClient = getConfig("query-coordinator-client", new QueryCoordinatorClientConfig(_, _))
  val geospaceClient = getConfig("geospace-client", new GeospaceClientConfig(_, _))
  val database = getConfig("database", new DataSourceConfig(_, _))
  val log4j = getRawConfig("log4j")
  // This is a Typesafe config because there are variable number of subentries, one per handler
  val handlers = getRawConfig("handlers")
  val metrics =  optionally(getConfig("metrics", new BalboaConfig(_,_)))
  val suggest = getConfig("suggest", new SuggestConfig(_,_))
  val codaMetrics = getRawConfig("metrics")
  val threadpool = getRawConfig("threadpool")
}

class DataCoordinatorClientConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val serviceName = getString("service-name")
  val instance = getString("instance")
  val connectTimeout = getDuration("connect-timeout")
  val receiveTimeout = getDuration("receive-timeout")
}

class QueryCoordinatorClientConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val serviceName = getString("service-name")
  val connectTimeout = getDuration("connect-timeout")
  val receiveTimeout = getDuration("receive-timeout")
}

class GeospaceClientConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val serviceName = getString("service-name")
  val connectTimeout = getDuration("connect-timeout")
  val readTimeout = getDuration("read-timeout")
}

class NetworkConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val port = getInt("port")
  val httpclient = getConfig("client", new HttpClientConfig(_, _))
}

class HttpClientConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val liveness = getConfig("liveness", new LivenessClientConfig(_, _))
}

class LivenessClientConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val interval = getDuration("interval")
  val range = getDuration("range")
  val missable = getInt("missable")
}

class DataSourceConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val host = getString("host")
  val port = getInt("port")
  val database = getString("database")
  val username = getString("username")
  val password = getString("password")
  val applicationName = getString("app-name")
  val poolOptions = optionally(getRawConfig("c3p0")) // these are the c3p0 configuration properties
}

class BalboaConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val activeMQConnectionUri = getString("activemq-connection-uri")
  val jmsQueue = getString("jms-queue")
}

class SuggestConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val host = getString("host")
  val port = getInt("port")
  val connectTimeout = getDuration("connect-timeout")
  val receiveTimeout = getDuration("receive-timeout")
}
