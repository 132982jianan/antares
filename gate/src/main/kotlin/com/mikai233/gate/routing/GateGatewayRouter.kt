package com.mikai233.gate.routing

import com.mikai233.common.runtime.support.system
import com.mikai233.gate.common.GateChannelActorKey
import com.mikai233.gate.message.ClientProtobuf
import com.mikai233.gate.node.GateNode
import io.github.realmlabs.asteria.cluster.pekko.EntityShardRegistry
import io.github.realmlabs.asteria.cluster.pekko.SingletonActorRegistry
import io.github.realmlabs.asteria.gateway.GatewayMessageDispatcher
import io.github.realmlabs.asteria.gateway.GatewayRoute
import io.github.realmlabs.asteria.gateway.GatewaySession
import io.github.realmlabs.asteria.gateway.GatewaySessionContext
import io.github.realmlabs.asteria.gateway.pekko.PekkoGatewayForwarder
import io.github.realmlabs.asteria.observability.Metrics
import io.github.realmlabs.asteria.observability.NoopMetrics

class GateGatewayRouter(
    private val node: GateNode,
) {
    private val routeResolver = GateGatewayRouteResolver()
    private val messageFactory = GateGatewayMessageFactory()
    private val localHandler = GateGatewayLocalHandler()
    private val metrics: Metrics
        get() = node.services.find(Metrics::class) ?: NoopMetrics

    fun dispatch(
        session: GatewaySession,
        packet: ClientProtobuf,
    ): GatewayRoute {
        return GatewayMessageDispatcher(
            routeResolver = routeResolver,
            forwarder = PekkoGatewayForwarder(
                system = node.system,
                shards = node.services.get(EntityShardRegistry::class),
                singletons = node.services.get(SingletonActorRegistry::class),
                messageFactory = messageFactory,
                localHandler = localHandler,
                sender = session.get(GateChannelActorKey),
                metrics = metrics,
            ),
            metrics = metrics,
        ).dispatch(GatewaySessionContext(session), packet)
    }
}
