package io.github.crabzilla.accounts.web

import io.github.crabzilla.accounts.CommandControllersFactory
import io.github.crabzilla.accounts.domain.accounts.AccountCommand.DepositMoney
import io.github.crabzilla.accounts.domain.accounts.AccountCommand.OpenAccount
import io.github.crabzilla.accounts.domain.accounts.AccountCommand.WithdrawMoney
import io.github.crabzilla.accounts.domain.transfers.TransferCommand.RequestTransfer
import io.github.crabzilla.accounts.web.CommandsResource.Companion.ID_PARAM
import io.github.crabzilla.pgclient.PgClientAbstractVerticle
import io.vertx.core.Promise
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import org.slf4j.LoggerFactory
import java.util.UUID

class WebVerticle : PgClientAbstractVerticle() {

  companion object {
    private val log = LoggerFactory.getLogger(WebVerticle::class.java)
  }

  private fun startAccountsResource(router: Router) {
    CommandsResource(CommandControllersFactory.accountsController(vertx, pgPool))
      .also { commandsResource ->
        router
          .put("/accounts/:$ID_PARAM/open")
          .handler {
            commandsResource.handle(it)
            { (metadata, body) ->
              OpenAccount(metadata.stateId, body.getString("cpf"), body.getString("name")) }
          }
        router
          .put("/accounts/:$ID_PARAM/deposit")
          .handler {
            commandsResource.handle(it) { (_, body) ->
              DepositMoney(body.getDouble("amount")) }
          }
        router
          .put("/accounts/:$ID_PARAM/withdraw")
          .handler {
            commandsResource.handle(it) { (_, body) -> WithdrawMoney(body.getDouble("amount")) }
          }
      }
  }

  private fun startTransferResource(router: Router) {
    CommandsResource(CommandControllersFactory.transfersController(vertx, pgPool))
      .also { commandsResource ->
        router
          .put("/transfers/:$ID_PARAM/request")
          .handler {
            commandsResource.handle(it)
            { (metadata, body) -> RequestTransfer(metadata.stateId,
              body.getDouble("amount"),
              UUID.fromString(body.getString("fromAccountId")),
              UUID.fromString(body.getString("toAccountId")),
            ) }
          }
      }
  }

  override fun start(startPromise: Promise<Void>) {

    val router = Router.router(vertx)

    router.route().handler(BodyHandler.create())

    startAccountsResource(router)
    startTransferResource(router)

    vertx
      .createHttpServer()
      .requestHandler(router)
      .listen(8888) { http ->
        if (http.succeeded()) {
          startPromise.complete()
          log.info("HTTP server started on port 8888")
        } else {
          startPromise.fail(http.cause())
        }
      }
  }

  override fun stop() {
    log.info("**** Stopped")
  }

}
