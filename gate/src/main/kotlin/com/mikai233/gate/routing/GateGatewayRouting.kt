package com.mikai233.gate.routing

import com.mikai233.common.runtime.support.GameEntityKinds
import io.github.realmlabs.asteria.core.EntityKind
import io.github.realmlabs.asteria.gateway.*
import io.github.realmlabs.asteria.message.RouteTarget

val GatePlayerIdKey: GatewaySessionAttributeKey<Long> = GatewaySessionAttributeKey("gate.playerId")
val GateWorldIdKey: GatewaySessionAttributeKey<Long> = GatewaySessionAttributeKey("gate.worldId")

val PlayerRouteTarget = RouteTarget.Entity(EntityKind(GameEntityKinds.PlayerActor))
val WorldRouteTarget = RouteTarget.Entity(EntityKind(GameEntityKinds.WorldActor))
val GmCommandTargets = mapOf(
    "testGm" to PlayerRouteTarget,
    "testBroadcast" to WorldRouteTarget,
)

fun requirePlayerId(session: GatewaySession): Long {
    return requireNotNull(session.get(GatePlayerIdKey)) {
        "playerId not bound for session ${session.id.value}"
    }
}

fun requireWorldId(session: GatewaySession): Long {
    return requireNotNull(session.get(GateWorldIdKey)) {
        "worldId not bound for session ${session.id.value}"
    }
}
