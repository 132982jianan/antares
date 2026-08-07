package com.mikai233.gate.routing

import com.mikai233.gate.generated.GeneratedGatewayRouting
import com.mikai233.gate.message.ClientProtobuf
import com.mikai233.protocol.ProtoSystem
import io.github.realmlabs.asteria.gateway.GatewayRoute
import io.github.realmlabs.asteria.gateway.GatewayRouteResolver
import io.github.realmlabs.asteria.gateway.GatewaySession
import io.github.realmlabs.asteria.gateway.GatewaySessionContext

class GateGatewayRouteResolver : GatewayRouteResolver<ClientProtobuf> {
    override fun resolve(context: GatewaySessionContext, packet: ClientProtobuf): GatewayRoute {
        GeneratedGatewayRouting.resolve(context, packet)?.let { return it }
        return when (val message = packet.message) {
            is ProtoSystem.GmReq -> resolveGmRoute(context.session, message)
            else -> error("no gateway route for packet id=${packet.id}, type=${packet.message::class.qualifiedName}")
        }
    }

    private fun resolveGmRoute(session: GatewaySession, request: ProtoSystem.GmReq): GatewayRoute {
        return when (GmCommandTargets[request.cmd]) {
            PlayerRouteTarget -> GatewayRoute(PlayerRouteTarget, requirePlayerId(session))
            WorldRouteTarget -> GatewayRoute(WorldRouteTarget, requireWorldId(session))
            null -> error("no gateway route for GM command=${request.cmd}")
            else -> error("unsupported gateway GM route for command=${request.cmd}")
        }
    }
}
