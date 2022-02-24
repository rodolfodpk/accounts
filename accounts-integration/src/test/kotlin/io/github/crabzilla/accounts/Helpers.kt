package io.github.crabzilla.accounts

import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Row
import io.vertx.sqlclient.RowSet

object Helpers {

  fun RowSet<Row>.asJson(key: String): JsonObject {
    val json = JsonObject()
    this.forEach {
      val rowAsJson = it.toJson()
      json.put(rowAsJson.getString(key), rowAsJson)
    }
    return json
  }

}