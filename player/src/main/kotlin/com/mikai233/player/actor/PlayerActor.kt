package com.mikai233.player.actor

import com.google.protobuf.GeneratedMessage
import com.mikai233.common.event.GameConfigChangedEvent
import com.mikai233.common.event.PlayerCreateEvent
import com.mikai233.common.event.PlayerLoginEvent
import com.mikai233.common.extension.ask
import com.mikai233.common.message.Message
import com.mikai233.common.runtime.recordMessageDispatch
import com.mikai233.common.runtime.support.GameEntityKinds
import com.mikai233.common.runtime.support.gameTimeSource
import com.mikai233.common.runtime.support.localEntityRegistry
import com.mikai233.common.runtime.support.system
import com.mikai233.common.time.ActorGameTime
import com.mikai233.player.common.PlayerDataManager
import com.mikai233.player.message.HandoffPlayer
import com.mikai233.player.message.PlayerTick
import com.mikai233.player.node.PlayerNode
import com.mikai233.protocol.ProtoLogin
import com.mikai233.protocol.ProtoRpcGate
import com.mikai233.protocol.ProtoRpcPlayer
import io.github.realmlabs.asteria.actor.ActorLifecycleGate
import io.github.realmlabs.asteria.actor.ActorTimerSupport
import io.github.realmlabs.asteria.actor.AsteriaActor
import io.github.realmlabs.asteria.message.dispatchActor
import io.github.realmlabs.asteria.script.pekko.ActorScriptSupport
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.Props
import org.apache.pekko.actor.ReceiveTimeout
import org.apache.pekko.cluster.sharding.ShardRegion
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/*
总结：
    PlayerActor 是一个有状态的玩家 Actor，基于 Pekko Cluster Sharding 运行。
    它管理玩家的在线/离线状态、数据生命周期（加载→运行→持久化→排空），并通过定时 Tick 驱动业务逻辑。
    消息处理通过 Protobuf 派发器和内部消息派发器两条路径分发，同时支持脚本扩展。
 */
class PlayerActor(val node: PlayerNode) : AsteriaActor<PlayerNode>(node) {
    //从当前 Actor 的路径名中解析出玩家 ID（因为 sharding 用 playerId 作为 entity id）
    val playerId: Long = self.path().name().toLong()

    //为该 Actor 创建一个独立的游戏时间源，用于时间相关的游戏逻辑
    val gameTime: ActorGameTime = node.gameTimeSource.actorTime()

    //持有与客户端通信的通道 Actor 引用，为 null 表示玩家不在线
    private var channelActor: ActorRef? = null

    //标记是否已开始停机流程，防止重复执行
    private var shutdownStarted = false

    //定时器支持，用于周期性任务（如 PlayerTick）
    private val timers = ActorTimerSupport(this)

    //脚本支持，允许通过脚本扩展 Actor 的消息处理
    private val scripts = ActorScriptSupport(this)

    //玩家数据管理器，负责数据的加载、保存、tick 等核心逻辑
    val manager = PlayerDataManager(this)

    //Actor 生命周期门控，管理加载（ load ）和排空（ drain ）两个阶段，确保数据在正确时机加载和持久化
    private val lifecycle = ActorLifecycleGate(
        owner = this,
        load = { manager.load() },
        drain = { manager.drain() },
    )

    override fun preStart() {
        super.preStart()

        //本地实体注册表注册此 PlayerActor
        node.localEntityRegistry.register(GameEntityKinds.PlayerActor, playerId.toString(), self)

        //启动定时器
        timers.start()

        //订阅 GameConfigChangedEvent 事件
        node.system.eventStream.subscribe(self, GameConfigChangedEvent::class.java)

        //开始加载数据（ lifecycle.startLoading()
        lifecycle.startLoading()

        logger.info("{} started", self)
    }

    override fun postStop() {
        super.postStop()

        //Actor 停止时：从注册表注销、打印日志
        node.localEntityRegistry.unregister(GameEntityKinds.PlayerActor, playerId.toString(), self)
        logger.info("{} stopped", self)
    }

    /*
    创建消息接收器。
     */
    override fun createReceive(): Receive {
        //在加载阶段使用 lifecycle.loadingReceive 拦截，加载完成后切换到 running() 并包裹脚本处理
        return lifecycle.loadingReceive {
            withScripts(running())
        }
    }

    /*
    进入运行状态时：启动 PlayerTick 定时器（每秒一次），设置接收超时为1分钟，然后返回活跃状态消息处理器
     */
    private fun running(): Receive {
        timers.startTimerWithFixedDelay(PlayerTick, PlayerTick, PlayerTickDuration_1_SEC)
        context.setReceiveTimeout(1.minutes.toJavaDuration())
        return active()
    }


    private fun active(): Receive {
        return receiveBuilder()
            //收到 HandoffPlayer （玩家迁移/交接指令）时，取消超时计时器并开始停机
            .match(HandoffPlayer::class.java) {
                context.cancelReceiveTimeout()
                lifecycle.beginStop()
            }

            //收到 PlayerTick 时，调用 manager.tick() 驱动业务逻辑
            .match(PlayerTick::class.java) { manager.tick() }
            //收到任意 Protobuf 消息，分发给 Protobuf 消息派发器
            .match(GeneratedMessage::class.java) { handleProtobufMessage(it) }
            //收到超时（1分钟无消息）且玩家不在线时，触发 passivate() （钝化/回收 Actor）
            .match(ReceiveTimeout::class.java) { if (!isOnline()) passivate() }

            //分发给内部消息派发器
            .match(GameConfigChangedEvent::class.java) { handlePlayerMessage(it) }
            .match(PlayerLoginEvent::class.java) { handlePlayerMessage(it) }
            .match(PlayerCreateEvent::class.java) { handlePlayerMessage(it) }
            .build()
    }

    /*
    将脚本的消息处理器叠加到当前 receive 上，脚本可以拦截未匹配的消息
     */
    private fun withScripts(receive: Receive): Receive {
        return receive.orElse(scripts.receive())
    }

    /*
    Protobuf 消息处理，通过 recordMessageDispatch 记录派发耗时，委托给 protobufDispatcher.dispatchActor()
     */
    private fun handleProtobufMessage(message: GeneratedMessage) {
        try {
            node.recordMessageDispatch("PlayerActor", "protobuf", message) {
                node.protobufDispatcher.dispatchActor(node, this, message)
            }
        } catch (e: Exception) {
            logger.error(e, "player:{} handle protobuf message:{} failed", playerId, message)
        }
    }

    /*
    内部消息处理，委托给 internalDispatcher.dispatchActor()
     */
    private fun handlePlayerMessage(message: Message) {
        try {
            node.recordMessageDispatch("PlayerActor", "internal", message) {
                node.internalDispatcher.dispatchActor(node, this, message)
            }
        } catch (e: Exception) {
            logger.error(e, "player:{} handle message:{} failed", playerId, message)
        }
    }

    /*
    判断玩家是否在线（有无绑定的通道 Actor）
     */
    fun isOnline(): Boolean {
        return channelActor != null
    }

    /*
    向客户端发送消息，通过 channelActor 转发
     */
    fun send(message: GeneratedMessage) {
        val boundChannelActor = channelActor
        if (boundChannelActor != null) {
            boundChannelActor.tell(message, self)
        } else {
            logger.warning("player:{} unable to send message to channel actor, because channel actor is null", playerId)
        }
    }

    /*
    请求 ShardRegion 钝化此 Actor（资源回收）
     */
    fun passivate() {
        context.parent.tell(ShardRegion.Passivate(HandoffPlayer), self)
    }

    /*
    绑定新的通道 Actor（玩家登录/重连时）。如果已有旧通道，先通知旧通道因多端登录踢下线（ MultiLogin 原因），再绑定新通道
     */
    fun bindChannelActor(incomingChannelActor: ActorRef) {
        if (incomingChannelActor != channelActor) {
            channelActor?.let {
                logger.info("player:{} unbind old channel actor:{}", playerId, it)
                it.tell(
                    ProtoRpcGate.ChannelExpiredReq.newBuilder()
                        .setReason(ProtoLogin.ConnectionExpiredNotify.Reason.MultiLogin_VALUE)
                        .build(),
                    self,
                )
            }
            channelActor = incomingChannelActor
            logger.info("player:{} bind new channel actor:{}", playerId, channelActor)
        }
    }

    /*
    清空通道 Actor 引用（玩家断线时）
     */
    fun clearChannelActor() {
        channelActor = null
    }

    /*
    按计划停机（用于运维/扩缩容）
     */
    fun shutdownForPlan(planId: String, coordinator: ActorRef) {
        //防止重复停机
        if (shutdownStarted) {
            return
        }

        //清空通道 Actor、取消超时
        shutdownStarted = true
        channelActor = null
        context.cancelReceiveTimeout()
        context.become(receiveBuilder().build())
        launch(timeout = null) {
            //异步执行 manager.flush() 持久化数据
            val result = runCatching { manager.flush() }

            //发送 PlayerShutdownAck 确认给协调者
            val ack = ProtoRpcPlayer.PlayerShutdownAck.newBuilder()
                .setPlayerId(playerId)
                .setShutdownPlanId(planId)
                .setSuccess(result.getOrDefault(false))
                .also { builder ->
                    result.exceptionOrNull()?.localizedMessage?.let(builder::setError)
                }
                .build()
            coordinator.tell(ack, self)

            //停止自身
            context.stop(self)
        }
    }

    /*
    向其他玩家 Actor 发消息（tell 模式，fire-and-forget）
     */
    fun tellPlayer(message: GeneratedMessage) {
        node.playerSharding.tell(message, self)
    }

    /*
    转发消息给其他玩家 Actor（保留原始 sender）
     */
    fun forwardPlayer(message: GeneratedMessage) {
        node.playerSharding.forward(message, context)
    }

    /*
    向其他玩家 Actor 发请求并等待响应（ask 模式，协程挂起）
     */
    suspend fun <R> askPlayer(message: GeneratedMessage): Result<R> {
        return node.playerSharding.ask(message)
    }

    fun tellWorld(message: GeneratedMessage) {
        node.worldSharding.tell(message, self)
    }

    fun forwardWorld(message: GeneratedMessage) {
        node.worldSharding.forward(message, context)
    }

    suspend fun <R> askWorld(message: GeneratedMessage): Result<R> {
        return node.worldSharding.ask(message)
    }

    /*
    生成全局唯一 ID
     */
    fun nextId(): Long {
        return node.idGenerator.nextId()
    }

    companion object {
        val PlayerTickDuration_1_SEC = 1.seconds

        fun props(node: PlayerNode): Props = Props.create(PlayerActor::class.java, node)
    }
}
