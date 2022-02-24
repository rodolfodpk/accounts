package io.github.crabzilla.accounts.domain.accounts

import io.github.crabzilla.accounts.domain.accounts.AccountCommand.DepositMoney
import io.github.crabzilla.accounts.domain.accounts.AccountCommand.OpenAccount
import io.github.crabzilla.accounts.domain.accounts.AccountCommand.WithdrawMoney
import io.github.crabzilla.accounts.domain.accounts.AccountEvent.AccountOpened
import io.github.crabzilla.accounts.domain.accounts.AccountEvent.MoneyDeposited
import io.github.crabzilla.accounts.domain.accounts.AccountEvent.MoneyWithdrawn
import io.github.crabzilla.core.command.CommandControllerConfig
import io.github.crabzilla.core.json.javaModule
import kotlinx.serialization.PolymorphicSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic

@kotlinx.serialization.ExperimentalSerializationApi
private val accountModule = SerializersModule {
    include(javaModule)
    polymorphic(Account::class) {
      subclass(Account::class, Account.serializer())
    }
    polymorphic(AccountCommand::class) {
      subclass(OpenAccount::class, OpenAccount.serializer())
      subclass(DepositMoney::class, DepositMoney.serializer())
      subclass(WithdrawMoney::class, WithdrawMoney.serializer())
    }
    polymorphic(AccountEvent::class) {
      subclass(AccountOpened::class, AccountOpened.serializer())
      subclass(MoneyDeposited::class, MoneyDeposited.serializer())
      subclass(MoneyWithdrawn::class, MoneyWithdrawn.serializer())
    }
}

val accountJson = Json { serializersModule = accountModule }

val accountConfig = CommandControllerConfig(
  PolymorphicSerializer(Account::class),
  PolymorphicSerializer(AccountCommand::class),
  PolymorphicSerializer(AccountEvent::class),
  accountEventHandler,
  { AccountCommandHandler() }
)