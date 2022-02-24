package io.github.crabzilla.accounts.web

import io.github.crabzilla.core.metadata.CommandMetadata
import io.github.crabzilla.pgclient.command.CommandController
import io.github.crabzilla.pgclient.command.CommandSideEffect
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import org.slf4j.LoggerFactory
import java.util.UUID

internal class CommandsResource<S: Any, C: Any, E: Any>(
  private val commandController: CommandController<S, C, E>)
{
  companion object {
    const val ID_PARAM: String = "id"
    private val log = LoggerFactory.getLogger(CommandsResource::class.java)
  }

  fun handle(ctx: RoutingContext, commandFactory: (Pair<CommandMetadata, JsonObject>) -> C) {
    val (metadata, body) = requestHandler(ctx)
    commandController.handle(metadata, commandFactory.invoke(Pair(metadata, body)))
      .onSuccess { successHandler(ctx, it) }
      .onFailure { errorHandler(ctx, it) }
  }

  private fun requestHandler(ctx: RoutingContext) : Pair<CommandMetadata, JsonObject> {
    val id = UUID.fromString(ctx.request().getParam(ID_PARAM))
    val metadata = CommandMetadata.new(id)
    return Pair(metadata, ctx.bodyAsJson)
  }

  private fun successHandler(ctx: RoutingContext, data: CommandSideEffect) {
    ctx.response().setStatusCode(201).end(JsonObject.mapFrom(data).encode())
  }

  private fun errorHandler(ctx: RoutingContext, error: Throwable) {
    log.error(ctx.request().absoluteURI(), error)
    // a silly convention, but hopefully effective for this demo
    when (error.cause) {
      is IllegalArgumentException -> ctx.response().setStatusCode(400).setStatusMessage(error.message).end()
      is NullPointerException -> ctx.response().setStatusCode(404).setStatusMessage(error.message).end()
      is IllegalStateException -> ctx.response().setStatusCode(409).setStatusMessage(error.message).end()
      else -> ctx.response().setStatusCode(500).setStatusMessage(error.message).end()
    }
  }

}