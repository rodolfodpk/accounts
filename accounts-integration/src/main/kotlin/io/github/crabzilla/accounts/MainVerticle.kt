package io.github.crabzilla.accounts

import com.hazelcast.config.Config
import io.github.crabzilla.accounts.integration.PendingTransfersVerticle
import io.github.crabzilla.pgclient.projection.deployProjector
import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.json.JsonObject
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.hazelcast.ConfigUtil
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import org.slf4j.LoggerFactory
import java.lang.management.ManagementFactory

class MainVerticle : AbstractVerticle() {

  companion object {
    private val log = LoggerFactory.getLogger(MainVerticle::class.java)
    private val node = ManagementFactory.getRuntimeMXBean().name
    val config = JsonObject("""
        {
          "targetDatabase": "accounts-db-config",
          "accounts-db-config" : {
            "port" : 5432,
            "host" : "0.0.0.0",
            "database" : "accounts",
            "user" : "user1",
            "password" : "pwd1",
            "pool" : {
              "maxSize": 12
            }
          }
        }
      """.trimIndent())
    @JvmStatic
    fun main(args: Array<String>) {
      Vertx.vertx().deployVerticle(MainVerticle(), DeploymentOptions().setConfig(config))
    }
  }

  override fun start(startPromise: Promise<Void>) {

    fun clusterMgr(): ClusterManager {
      val hazelcastConfig: Config = ConfigUtil.loadConfig().setLiteMember(false)
      hazelcastConfig.setProperty("hazelcast.logging.type", "slf4j")
//      hazelcastConfig.cpSubsystemConfig.cpMemberCount = 3
      return HazelcastClusterManager(hazelcastConfig)
    }

    val options = VertxOptions().setClusterManager(clusterMgr()).setHAEnabled(true)
    Vertx.clusteredVertx(options)
      .compose { vertx ->
        log.info(config.encodePrettily())
        vertx.deployProcessor(config, PendingTransfersVerticle::class.java)
          .compose {
            vertx.deployProjector(config, "service:integration.projectors.accounts.AccountsView")
          }
          .compose {
            vertx.deployProjector(config, "service:integration.projectors.transfers.TransfersView")
          }
          .onFailure {
            startPromise.fail(it)
            log.error(it.message, it)
          }
          .onSuccess {
            startPromise.complete()
          }
      }

  }

  override fun stop() {
    log.info("**** Stopped")
  }

}
