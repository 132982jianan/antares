package com.mikai233.global.actor

import com.mikai233.common.message.Message
import com.mikai233.global.node.GlobalNode
import com.mikai233.global.message.HandoffWorker
import io.github.realmlabs.asteria.actor.AsteriaActor
import io.github.realmlabs.asteria.script.pekko.ActorScriptSupport
import org.apache.pekko.actor.Props

/*
总结 ： WorkerActor 是一个基于 Pekko 的 Actor 封装，继承自 Asteria 框架基类。
        它处理两类消息： HandoffWorker （触发 Actor 自停，用于 Worker 交接）和通用 Message （默认打印 warning，期望被子类或脚本覆盖）。
        同时集成了 ActorScriptSupport 作为兜底消息处理机制。
 */
class WorkerActor(val node: GlobalNode) : AsteriaActor<GlobalNode>(node) {
    private val scripts = ActorScriptSupport(this)

    override fun preStart() {
        super.preStart()
        logger.info("{} started", self)
    }

    override fun postStop() {
        super.postStop()
        logger.info("{} stopped", self)
    }

    override fun createReceive(): Receive {
        return receiveBuilder()
            //收到 HandoffWorker 消息时，停止当前 Actor（用于 Worker 交接/下线）
            .match(HandoffWorker::class.java) { context.stop(self) }
            //收到通用的 Message 消息时，调用 handleMessage 处理
            .match(Message::class.java) { handleMessage(it) }
            .build()
            //以上都不匹配时，交给 scripts （ActorScriptSupport）尝试动态处理
            .orElse(scripts.receive())
    }

    private fun handleMessage(message: Message) {
        logger.warning("WorkerActor received unsupported message: {}", message)
    }

    companion object {
        //创建 Pekko Props ，用于通过 ActorSystem 创建 WorkerActor 实例
        fun props(node: GlobalNode): Props = Props.create(WorkerActor::class.java, node)
    }
}
