package io.github.crabzilla.accounts.processors

import io.github.crabzilla.accounts.domain.accounts.Account
import io.github.crabzilla.accounts.domain.accounts.AccountCommand
import io.github.crabzilla.accounts.domain.accounts.AccountCommand.DepositMoney
import io.github.crabzilla.accounts.domain.accounts.AccountEvent
import io.github.crabzilla.accounts.domain.transfers.Transfer
import io.github.crabzilla.accounts.domain.transfers.TransferCommand
import io.github.crabzilla.accounts.domain.transfers.TransferCommand.RegisterResult
import io.github.crabzilla.accounts.domain.transfers.TransferEvent
import io.github.crabzilla.core.metadata.CommandMetadata
import io.github.crabzilla.pgclient.command.CommandController
import io.github.crabzilla.pgclient.command.CommandSideEffect
import io.vertx.core.Future
import io.vertx.core.Promise
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID

class TransferService(
  private val acctController: CommandController<Account, AccountCommand, AccountEvent>,
  private val transferController: CommandController<Transfer, TransferCommand, TransferEvent>) {
  
  companion object {
    private val log: Logger = LoggerFactory.getLogger(TransferService::class.java)
  }

  data class PendingTransfer(
    val id: UUID, val amount: Double, val fromAccountId: UUID, val toAccountId: UUID,
    val causationId: UUID, val correlationId: UUID,
  )

  /**
   * Steps within the same db transaction:
   * fromAcctId withdrawn
   * toAcctId deposit
   * transferId register success
   * in case of error, the failure will be registered into a new db tx
   */
  fun transfer(pendingTransfer: PendingTransfer): Future<Void> {

    val promise = Promise.promise<Void>()
    val transferId = pendingTransfer.id
    val correlationId = pendingTransfer.correlationId

    acctController.compose { conn ->
      log.info("Step 1 - Will withdrawn from account {}", pendingTransfer.fromAccountId)
      val withdrawnMetadata = CommandMetadata.new(
        stateId = pendingTransfer.fromAccountId,
        causationId = pendingTransfer.causationId,
        correlationId = correlationId
      )
      val withdrawnCommand = AccountCommand.WithdrawMoney(pendingTransfer.amount)
      acctController.handle(conn, withdrawnMetadata, withdrawnCommand)
        .compose { r1: CommandSideEffect ->
          log.info("Step 2 - Will deposit to account {}", pendingTransfer.toAccountId)
          val depositMetadata = CommandMetadata.new(
            stateId = pendingTransfer.toAccountId,
            causationId = r1.appendedEvents.last().second.eventId,
            correlationId = correlationId)
          val depositCommand = DepositMoney(pendingTransfer.amount)
          acctController.handle(conn, depositMetadata, depositCommand)
        }.compose { r2: CommandSideEffect ->
          log.info("Step 3 - Will register a succeeded transfer")
          val registerSuccessMetadata = CommandMetadata.new(
            stateId = transferId,
            causationId = r2.appendedEvents.last().second.eventId,
            correlationId = correlationId)
          val registerSuccessCommand = RegisterResult(true, null)
          transferController.handle(conn, registerSuccessMetadata, registerSuccessCommand)
            .map { r2 }
        }.onSuccess {
          promise.complete()
        }.onFailure { error ->
          // new transaction
          log.info("Step 3 - Will register a failed transfer", error)
          val registerFailureMetadata = CommandMetadata.new(
            stateId = transferId,
            causationId = correlationId,
            correlationId = correlationId)
          val registerFailureCommand = RegisterResult(false, error.message)
          transferController.handle(registerFailureMetadata, registerFailureCommand)
            .onSuccess { promise.complete() }
        }
    }
    return promise.future()

  }
}