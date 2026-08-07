package com.mikai233.gate.routing

import com.mikai233.common.runtime.support.GameEntityKinds
import com.mikai233.gate.generated.GeneratedGatewayRouting
import com.mikai233.gate.message.ClientProtobuf
import com.mikai233.protocol.ProtoSystem
import io.github.realmlabs.asteria.core.EntityKind
import io.github.realmlabs.asteria.gateway.GatewayRoute
import io.github.realmlabs.asteria.gateway.GatewaySessionContext
import io.github.realmlabs.asteria.gateway.pekko.PekkoGatewayMessageFactory
import io.github.realmlabs.asteria.message.RouteTarget

class GateGatewayMessageFactory : PekkoGatewayMessageFactory<ClientProtobuf> {
    override fun entityMessage(context: GatewaySessionContext, route: GatewayRoute, packet: ClientProtobuf): Any {
        GeneratedGatewayRouting.entityMessage(context, route, packet)?.let { return it }
        val target = route.target as? RouteTarget.Entity
            ?: error("expected entity route target but got ${route.target}")
        return when (target.kind) {
            EntityKind(GameEntityKinds.PlayerActor) -> {
                when (val message = packet.message) {
                    is ProtoSystem.GmReq -> message.toBuilder()
                        .setPlayerId(route.entityId as Long)
                        .clearWorldId()
                        .build()

                    else -> error("unsupported player gateway message ${packet.message::class.qualifiedName}")
                }
            }

            EntityKind(GameEntityKinds.WorldActor) -> {
                when (val message = packet.message) {
                    is ProtoSystem.GmReq -> message.toBuilder()
                        .setPlayerId(requirePlayerId(context.session))
                        .setWorldId(route.entityId as Long)
                        .build()

                    else -> error("unsupported world gateway message ${packet.message::class.qualifiedName}")
                }
            }

            else -> error("unsupported gateway entity target ${target.kind.value}")
        }
    }

    override fun singletonMessage(context: GatewaySessionContext, route: GatewayRoute, packet: ClientProtobuf): Any {
        error("gate does not use singleton gateway routes")
    }

    override fun serviceMessage(context: GatewaySessionContext, route: GatewayRoute, packet: ClientProtobuf): Any {
        error("gate does not use service gateway routes")
    }

    override fun localMessage(context: GatewaySessionContext, route: GatewayRoute, packet: ClientProtobuf): Any {
        return LocalClientProtobuf(packet.message)
    }
}
