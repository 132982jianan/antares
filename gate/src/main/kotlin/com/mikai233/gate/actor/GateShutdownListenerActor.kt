package com.mikai233.gate.actor

import com.mikai233.common.extension.decodeActorRef
import com.mikai233.common.runtime.support.system
import com.mikai233.common.shutdown.GATE_DRAIN_TOPIC
import com.mikai233.gate.node.GateNode
import com.mikai233.protocol.ProtoRpcShutdown
import io.github.realmlabs.asteria.actor.AsteriaActor
import org.apache.pekko.actor.Props
import org.apache.pekko.cluster.pubsub.DistributedPubSub
import org.apache.pekko.cluster.pubsub.DistributedPubSubMediator

class GateShutdownListenerActor(val node: GateNode) : AsteriaActor<GateNode>(node) {
    private val mediator = DistributedPubSub.get(context.system).mediator()

    override fun preStart() {
        super.preStart()
        mediator.tell(DistributedPubSubMediator.Subscribe(GATE_DRAIN_TOPIC, self), self)
    }

    override fun createReceive(): Receive {
        return receiveBuilder()
            .match(ProtoRpcShutdown.GateDrainCommand::class.java) { handleGateDrain(it) }
            .build()
    }

    private fun handleGateDrain(command: ProtoRpcShutdown.GateDrainCommand) {
        val coordinator = command.coordinatorActor.decodeActorRef(node.system)
        node.connectionDrainer.beginDrain(
            planId = command.planId,
            coordinator = coordinator,
            reason = "shutdown plan ${command.planId}",
        )
        val playerIds = node.connectionDrainer.activePlayerIds
        node.connectionDrainer.closeAll()
        coordinator.tell(
            ProtoRpcShutdown.GateDrainAck.newBuilder()
                .setPlanId(command.planId)
                .setGateNodeId(node.nodeId)
                .addAllPlayerId(playerIds)
                .build(),
            self,
        )
    }

    companion object {
        const val Name = "gateShutdownListener"

        fun props(node: GateNode): Props = Props.create(GateShutdownListenerActor::class.java, node)
    }
}
