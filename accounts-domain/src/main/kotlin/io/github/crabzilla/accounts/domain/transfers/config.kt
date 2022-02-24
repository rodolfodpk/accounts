package io.github.crabzilla.accounts.domain.transfers

import io.github.crabzilla.accounts.domain.transfers.TransferCommand.RegisterResult
import io.github.crabzilla.accounts.domain.transfers.TransferCommand.RequestTransfer
import io.github.crabzilla.accounts.domain.transfers.TransferEvent.TransferConcluded
import io.github.crabzilla.accounts.domain.transfers.TransferEvent.TransferRequested
import io.github.crabzilla.core.command.CommandControllerConfig
import io.github.crabzilla.core.json.javaModule
import kotlinx.serialization.PolymorphicSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic

/**
 * kotlinx.serialization
 */
@kotlinx.serialization.ExperimentalSerializationApi
private val transferModule = SerializersModule {
  include(javaModule)
  polymorphic(Transfer::class) {
    subclass(Transfer::class, Transfer.serializer())
  }
  polymorphic(TransferCommand::class) {
    subclass(RequestTransfer::class, RequestTransfer.serializer())
    subclass(RegisterResult::class, RegisterResult.serializer())
  }
  polymorphic(TransferEvent::class) {
    subclass(TransferRequested::class, TransferRequested.serializer())
    subclass(TransferConcluded::class, TransferConcluded.serializer())
  }
}

val transferJson = Json { serializersModule = transferModule }

val transferConfig = CommandControllerConfig(
  PolymorphicSerializer(Transfer::class),
  PolymorphicSerializer(TransferCommand::class),
  PolymorphicSerializer(TransferEvent::class),
  transferEventHandler,
  { TransferCommandHandler() }
)
