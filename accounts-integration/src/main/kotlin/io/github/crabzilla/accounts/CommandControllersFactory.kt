package io.github.crabzilla.accounts

import io.github.crabzilla.accounts.domain.accounts.Account
import io.github.crabzilla.accounts.domain.accounts.AccountCommand
import io.github.crabzilla.accounts.domain.accounts.AccountCommandHandler
import io.github.crabzilla.accounts.domain.accounts.AccountEvent
import io.github.crabzilla.accounts.domain.accounts.AccountsSerialization
import io.github.crabzilla.accounts.domain.accounts.accountEventHandler
import io.github.crabzilla.accounts.domain.transfers.Transfer
import io.github.crabzilla.accounts.domain.transfers.TransferCommand
import io.github.crabzilla.accounts.domain.transfers.TransferCommandHandler
import io.github.crabzilla.accounts.domain.transfers.TransferEvent
import io.github.crabzilla.accounts.domain.transfers.TransfersSerialization
import io.github.crabzilla.accounts.domain.transfers.transferEventHandler
import io.github.crabzilla.accounts.processors.AccountOpenedProjector
import io.github.crabzilla.core.command.CommandControllerConfig
import io.github.crabzilla.json.KotlinJsonSerDer
import io.github.crabzilla.pgclient.command.CommandController
import io.github.crabzilla.pgclient.command.SnapshotType
import io.vertx.core.Vertx
import io.vertx.pgclient.PgPool
import kotlinx.serialization.json.Json

object CommandControllersFactory {

  fun accountsController(vertx: Vertx, pgPool: PgPool): CommandController<Account, AccountCommand, AccountEvent> {
    val accountJson = Json { serializersModule = AccountsSerialization.accountModule }
    val accountConfig = CommandControllerConfig("Account", accountEventHandler, { AccountCommandHandler() })
    return CommandController.create(vertx, pgPool, KotlinJsonSerDer(accountJson),
      accountConfig, SnapshotType.ON_DEMAND, AccountOpenedProjector("accounts_view"))
  }

  fun transfersController(vertx: Vertx, pgPool: PgPool): CommandController<Transfer, TransferCommand, TransferEvent> {
    val transferJson = Json { serializersModule = TransfersSerialization.transferModule }
    val transferConfig = CommandControllerConfig("Transfer", transferEventHandler, { TransferCommandHandler() })
    return CommandController.create(vertx, pgPool, KotlinJsonSerDer(transferJson),
      transferConfig, SnapshotType.ON_DEMAND)
  }

}