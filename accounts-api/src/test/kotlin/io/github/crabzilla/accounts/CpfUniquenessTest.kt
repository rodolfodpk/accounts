package io.github.crabzilla.accounts

import io.github.crabzilla.accounts.domain.accounts.AccountCommand
import io.github.crabzilla.core.metadata.CommandMetadata
import io.github.crabzilla.pgclient.PgClientFactory
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.pgclient.PgPool
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import java.util.UUID

@ExtendWith(VertxExtension::class)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class CpfUniquenessTest {

  companion object {

    private val log = LoggerFactory.getLogger(CpfUniquenessTest::class.java)

    // create acct1
    val md1 = CommandMetadata(UUID.randomUUID())
    val cmd1 = AccountCommand.OpenAccount(md1.stateId, "cpf1", "person1")

    // create acct2 with same cpf
    val md2 = CommandMetadata(UUID.randomUUID())
    val cmd2 = AccountCommand.OpenAccount(md2.stateId, "cpf1", "person2")

    fun configRetriever(vertx: Vertx): ConfigRetriever {
      val fileStore = ConfigStoreOptions()
        .setType("file")
        .setConfig(JsonObject().put("path", "./../conf/config.test.json"))
      val options = ConfigRetrieverOptions().addStore(fileStore)
      return ConfigRetriever.create(vertx, options)
    }

    fun pgPool(vertx: Vertx, config: JsonObject): PgPool {
      val configId = config.getString("connectOptionsName")
      val connectOptions = PgClientFactory.createPgConnectOptions(config.getJsonObject(configId))
      val poolOptions = PgClientFactory.createPoolOptions(config.getJsonObject(configId))
      return PgPool.pool(vertx, connectOptions, poolOptions)
    }

    @BeforeAll
    @JvmStatic
    fun deployMainVerticle(vertx: Vertx, testContext: VertxTestContext) {
      configRetriever(vertx).config
        .onSuccess { config ->
          vertx.deployVerticle(MainVerticle(), DeploymentOptions().setConfig(config), testContext.succeeding {
            testContext.completeNow()
          }
          )
        }
    }

  }



  private fun cleanDatabase(pgPool: PgPool): Future<Void> {
    return pgPool
      .query("delete from commands").execute()
      .compose { pgPool.query("delete from events").execute() }
      .compose { pgPool.query("delete from snapshots").execute() }
      .compose { pgPool.query("delete from accounts_view").execute() }
      .compose { pgPool.query("delete from transfers_view").execute() }
      .compose { pgPool.query("update projections set sequence = 0").execute() }
      .mapEmpty()
  }

  @Test
  fun `when opening 2 accounts with same cpf, it should fail`(vertx: Vertx, tc: VertxTestContext) {
    configRetriever(vertx).config
      .compose { config ->
        val pgPool = pgPool(vertx, config)
        val acctController = CommandControllersFactory.accountsController(vertx , pgPool)
        cleanDatabase(pgPool)
          .compose {
            log.info("Will handle {}", cmd1)
            acctController.handle(md1, cmd1)
              .compose {
                log.info("Will handle {}", cmd2)
                acctController.handle(md2, cmd2)
              }
          }
          .onSuccess {
            tc.failNow("Should fail since cpf uniqueness violation")
          }
          .onFailure { tc.completeNow() }
      }
  }

}
