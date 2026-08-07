package com.mikai233.gate.routing

import com.mikai233.gate.common.GateChannelActorKey
import com.mikai233.gate.message.ClientProtobuf
import io.github.realmlabs.asteria.gateway.GatewayRoute
import io.github.realmlabs.asteria.gateway.GatewaySessionContext
import io.github.realmlabs.asteria.gateway.pekko.PekkoGatewayLocalHandler
import org.apache.pekko.actor.ActorRef

class GateGatewayLocalHandler : PekkoGatewayLocalHandler<ClientProtobuf> {
    override fun handle(context: GatewaySessionContext, route: GatewayRoute, packet: ClientProtobuf) {
        val channelActor = context.session.get(GateChannelActorKey)
            ?: error("channel actor not found for session ${context.session.id.value}")
        channelActor.tell(LocalClientProtobuf(packet.message), ActorRef.noSender())
    }
}
