package io.github.crabzilla.accounts.integration

import io.github.crabzilla.core.metadata.EventMetadata
import io.github.crabzilla.pgclient.EventsProjector
import io.vertx.core.Future
import io.vertx.core.Future.succeededFuture
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.SqlConnection
import io.vertx.sqlclient.Tuple
import java.util.UUID

// TODO propagate causation and correlation ids
class AccountOpenedProjector(override val viewName: String) : EventsProjector {

  override fun project(conn: SqlConnection, eventAsJson: JsonObject, eventMetadata: EventMetadata): Future<Void> {
    fun register(conn: SqlConnection, id: UUID, cpf: String, name: String): Future<Void> {
      return conn
        .preparedQuery("insert into $viewName (id, cpf, name) values ($1, $2, $3) returning id")
        .execute(Tuple.of(id, cpf, name))
        .mapEmpty()
    }

    val id = eventMetadata.stateId
    return when (eventAsJson.getString("type")) {
      "AccountOpened" ->
        register(conn, id, eventAsJson.getString("cpf"), eventAsJson.getString("name"))
      else ->
        succeededFuture() // ignore event
    }
  }
}

