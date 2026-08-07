package com.mikai233.gate.actor

import com.google.protobuf.GeneratedMessage
import com.google.protobuf.kotlin.toByteString
import com.mikai233.common.broadcast.Topic
import com.mikai233.common.conf.ServerMode
import com.mikai233.common.extension.encodeActorRef
import com.mikai233.common.extension.invokeOnTargetMode
import com.mikai233.common.extension.tell
import com.mikai233.common.message.formatMessage
import com.mikai233.common.runtime.recordMessageDispatch
import com.mikai233.common.runtime.support.gameTimeSource
import com.mikai233.common.runtime.support.playerBroadcastEventBus
import com.mikai233.common.runtime.support.system
import com.mikai233.common.time.ActorGameTime
import com.mikai233.gate.common.ChannelState
import com.mikai233.gate.common.GatePlayerIdKey
import com.mikai233.gate.common.GateWorldIdKey
import com.mikai233.gate.common.LocalClientProtobuf
import com.mikai233.gate.common.closeGateChannel
import com.mikai233.gate.common.enableGateCipher
import com.mikai233.gate.node.GateNode
import com.mikai233.gate.crypto.AESCipher
import com.mikai233.gate.crypto.ECDH
import com.mikai233.gate.message.ChannelExpired
import com.mikai233.gate.message.ClientProtobuf
import com.mikai233.gate.message.StopChannel
import com.mikai233.protocol.ProtoLogin
import com.mikai233.protocol.ProtoLogin.LoginReq
import com.mikai233.protocol.ProtoLogin.LoginResp
import com.mikai233.protocol.ProtoRpcGate.ChannelExpiredReq
import com.mikai233.protocol.ProtoRpcPlayer.PlayerChannelClosedReq
import com.mikai233.protocol.connectionExpiredNotify
import io.github.realmlabs.asteria.actor.AsteriaActor
import io.github.realmlabs.asteria.gateway.GatewaySession
import io.github.realmlabs.asteria.message.dispatchActor
import io.github.realmlabs.asteria.script.pekko.ActorScriptSupport
import org.apache.pekko.actor.Props
import org.apache.pekko.actor.ReceiveTimeout
import org.apache.pekko.cluster.pubsub.DistributedPubSub
import org.apache.pekko.cluster.pubsub.DistributedPubSubMediator.Subscribe
import org.apache.pekko.cluster.pubsub.DistributedPubSubMediator.Unsubscribe
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

/*
这是一个管理客户端连接通道的 Pekko Actor，状态机包含三个阶段： Connecting → Authenticating → Authorized 。主要流程是：接收客户端 LoginReq → ECDH 密钥交换 → 启用加密 → 授权通道。
 */
class ChannelActor(val node: GateNode, private val session: GatewaySession) : AsteriaActor<GateNode>(node) {
    //可选类型合理，登录前为 null
    var playerId: Long? = null

    //私有，仅内部使用
    private var worldId: Long? = null

    //在 handleClientConnectMessage 中赋值。由于 actor 是单线程的，登录流程保证了先设置再使用，实际不会有 UninitializedPropertyAccessException 风险。
    private lateinit var clientPublicKey: ByteArray

    //DistributedPubSub 中介者
    private val mediator = DistributedPubSub.get(context.system).mediator()

    //脚本扩展点
    private val scripts = ActorScriptSupport(this)

    //追踪已订阅主题，用于清理
    private val subscribedTopics = mutableSetOf<String>()

    //初始状态
    private var state = ChannelState.Connecting

    //游戏时间源
    val gameTime: ActorGameTime = node.gameTimeSource.actorTime()

    override fun preStart() {
        super.preStart()
        logger.info("{} started", remoteActorRefAddress())

        //设置 1 分钟空闲超时，防止僵尸连接
        context.setReceiveTimeout(MaxIdleDuration_1MIN.toJavaDuration())

        //预订阅全局主题和 WORLD_ACTIV
        subscribe(Topic.All_WORLDS_TOPIC)
        mediator.tell(Subscribe(Topic.WORLD_ACTIVE, self))
    }

    override fun postStop() {
        super.postStop()
        //通知玩家通道关闭
        notifyPlayerChannelClosed()
        //取消所有订阅
        unsubscribeAll()
        //取消 WORLD_ACTIVE 订阅
        mediator.tell(Unsubscribe(Topic.WORLD_ACTIVE, self))
        logger.info("{} stopped", remoteActorRefAddress())
    }

    /*
    消息路由匹配器，按类型分发
     */
    override fun createReceive(): Receive {
        return receiveBuilder()
            //客户端消息
            .match(ClientProtobuf::class.java) { handleClientMessage(it) }
            //本地客户端消息
            .match(LocalClientProtobuf::class.java) { handleLocalClientMessage(it) }
            //服务端消息
            .match(GeneratedMessage::class.java) { handleServerMessage(it) }
            //停止通道
            .match(StopChannel::class.java) { stopChannel() }
            //通道过期
            .match(ChannelExpired::class.java) { expire(it.reason) }
            //空闲超时也走停止通道
            .match(ReceiveTimeout::class.java) { stopChannel() }
            .build()
            //合并脚本扩展
            .let(::withScripts)
    }

    /*
    简单的编码+写入封装
     */
    fun write(message: GeneratedMessage) {
        session.write(node.protocolCodec.encodeServer(message))
    }

    /*
    点评 ：状态机设计清晰，状态转换通过 when 表达式严格控制。
     */
    private fun handleClientMessage(clientProtobuf: ClientProtobuf) {
        when (state) {
            //只接受 LoginReq，否则停止-警告日志
            ChannelState.Connecting -> handleClientConnectMessage(clientProtobuf)
            //拒绝并停止-处理 LoginResp/ChannelExpiredReq，其余 stash
            ChannelState.Authenticating -> handleAuthenticatingClientMessage(clientProtobuf)
            //转发到 gatewayRouter-可分发的走 dispatcher，否则写入客户端
            ChannelState.Authorized -> forwardClientMessage(clientProtobuf)
        }
    }

    /*
    提取客户端公钥和 worldId，切换到认证状态，将登录请求转发到 world sharding。
     */
    private fun handleClientConnectMessage(clientProtobuf: ClientProtobuf) {
        val message = clientProtobuf.message
        if (message !is LoginReq) {
            logger.warning(
                "{} receive unexpected client side message:{} when not authorized",
                self,
                formatMessage(message),
            )
            stopChannel()
            return
        }
        clientPublicKey = message.clientPublicKey.toByteArray()
        worldId = message.worldId
        state = ChannelState.Authenticating
        node.worldSharding.tell(message, self)
    }

    private fun handleAuthenticatingClientMessage(clientProtobuf: ClientProtobuf) {
        logger.warning(
            "{} unexpected client message:{} while authenticating, stop the channel",
            self,
            formatMessage(clientProtobuf.message),
        )
        stopChannel()
    }

    private fun handleServerMessage(message: GeneratedMessage) {
        when (state) {
            ChannelState.Connecting -> {
                logger.warning(
                    "{} receive unexpected server side message:{} when not authorized",
                    self,
                    formatMessage(message),
                )
            }

            ChannelState.Authenticating -> handleAuthenticatingServerMessage(message)
            ChannelState.Authorized -> handleAuthorizedGeneratedMessage(message)
        }
    }

    private fun handleAuthenticatingServerMessage(message: GeneratedMessage) {
        when (message) {
            is LoginResp -> handleLoginResp(message)
            is ChannelExpiredReq -> dispatchProtobufMessage(message)
            else -> stash()
        }
    }

    /*
    处理登录结果
     */
    private fun handleLoginResp(resp: LoginResp) {
        when (resp.result) {
            //Success 走成功流程，
            ProtoLogin.LoginResult.Success -> {
                handleLoginSuccess(resp)
            }

            //走失败流程，
            ProtoLogin.LoginResult.RegisterLimit,
            ProtoLogin.LoginResult.WorldNotExists,
            ProtoLogin.LoginResult.WorldClosed,
            ProtoLogin.LoginResult.AccountBan,
                -> {
                handleLoginFailed(resp)
            }

            //也当失败处理
            ProtoLogin.LoginResult.UNRECOGNIZED, null -> {
                logger.error("unknown login result, stop the channel")
                handleLoginFailed(resp)
            }
        }
    }

    private fun handleLoginSuccess(resp: LoginResp) {
        //取消空闲超时（已成功认证）
        context.cancelReceiveTimeout()
        val playerData = resp.data

        //将 playerId/worldId 存入 session
        playerId = playerData.playerId
        session.set(GatePlayerIdKey, playerData.playerId)
        session.set(GateWorldIdKey, requireNotNull(worldId) { "worldId is null" })

        //生成 ECDH 服务端密钥对，发送公钥给客户端
        val serverKeyPair = ECDH.genKeyPair()
        val keyResp = resp.toBuilder().setServerPublicKey(serverKeyPair.publicKey.toByteString()).build()
        runCatching {
            write(keyResp)
        }.onFailure {
            //- 发送失败则停止通道
            logger.error(it, "write server key to client failed, stop the channel")
            stopChannel()
        }.onSuccess {
            //成功发送后计算共享密钥，启用 AES 加密
            val shareKey = ECDH.calculateSharedKey(serverKeyPair.privateKey, clientPublicKey)
            session.enableGateCipher(AESCipher(shareKey))
            //调用 authorizeChannel()
            authorizeChannel()
        }
    }

    private fun handleLoginFailed(resp: LoginResp) {
        write(resp)
        stopChannel()
    }

    private fun authorizeChannel() {
        //订阅特定世界话题和跨世界聊天
        subscribe(Topic.ofWorld(requireNotNull(worldId) { "worldId is null" }))
        subscribe(Topic.CROSS_WORLD_CHAT)
        //切换到授权状态
        state = ChannelState.Authorized
        //释放之前 stash 的消息
        unstashAll()
    }

    private fun withScripts(receive: Receive): Receive {
        return receive.orElse(scripts.receive())
    }

    fun expire(reason: Int) {
        write(connectionExpiredNotify { reasonValue = reason })
        stopChannel()
    }

    /**
     * 断开和客户端的连接
     */
    private fun stopChannel() {
        session.closeGateChannel()
        context.stop(self)
    }

    private fun forwardClientMessage(clientProtobuf: ClientProtobuf) {
        logger.debug("forward message:{}", formatMessage(clientProtobuf.message))
        try {
            //将客户端消息路由到网关处理器，异常被捕获并记录日志，不会导致 actor 崩溃
            node.gatewayRouter.dispatch(session, clientProtobuf)
        } catch (e: Exception) {
            logger.error(e, "channel:{} forward client message:{} failed", self, clientProtobuf.message)
        }
    }

    private fun handleLocalClientMessage(message: LocalClientProtobuf) {
        if (state != ChannelState.Authorized) {
            logger.warning(
                "{} receive unexpected local client message:{} when not authorized",
                self,
                formatMessage(message.message),
            )
            stopChannel()
            return
        }
        dispatchProtobufMessage(message.message)
    }

    private fun dispatchProtobufMessage(message: GeneratedMessage) {
        try {
            node.recordMessageDispatch("ChannelActor", "protobuf", message) {
                node.protobufDispatcher.dispatchActor(node, this, message)
            }
        } catch (e: Exception) {
            logger.error(e, "channel:{} handle protobuf message:{} failed", self, message)
        }
    }

    private fun handleAuthorizedGeneratedMessage(message: GeneratedMessage) {
        //优先尝试通过 protobuf dispatcher 本地分发
        if (node.protobufDispatcher.canDispatch(message::class)) {
            dispatchProtobufMessage(message)
            return
        }

        //无法分发则写入客户端（转发到下游）
        invokeOnTargetMode(node.runtimeEnv.serverMode, ServerMode.DevMode) {
            logger.info(
                "{} playerId:{} worldId:{} receive server message:{}",
                remoteActorRefAddress(),
                playerId,
                worldId,
                formatMessage(message),
            )
        }
        write(message)
    }

    /*
    获取带完整地址的 actor 路径用于日志显示。
     */
    private fun remoteActorRefAddress(): String {
        val path = self.path().toStringWithAddress(node.system.provider().defaultAddress)
        return "Actor[$path]"
    }

    /*
    主题订阅管理
     */
    fun subscribe(topic: String) {
        subscribedTopics.add(topic)
        node.playerBroadcastEventBus.subscribe(self, topic)
    }

    /*
     */
    fun unsubscribe(topic: String) {
        subscribedTopics.remove(topic)
        node.playerBroadcastEventBus.unsubscribe(self, topic)
    }

    /*
    在 postStop 时调用，确保资源清理。
     */
    private fun unsubscribeAll() {
        subscribedTopics.forEach { topic ->
            node.playerBroadcastEventBus.unsubscribe(self, topic)
        }
        subscribedTopics.clear()
    }

    private fun notifyPlayerChannelClosed() {
        //playerId 为 null 时直接返回（通道可能在登录前关闭）
        val currentPlayerId = playerId ?: return
        val drainContext = node.connectionDrainer.drainContext

        //构建 PlayerChannelClosedReq ，包含 drain context 信息（优雅关闭场景）
        val requestBuilder = PlayerChannelClosedReq
            .newBuilder()
            .setPlayerId(currentPlayerId)
        if (drainContext != null) {
            requestBuilder
                .setShutdown(true)
                .setShutdownPlanId(drainContext.planId)
                .setCoordinatorActor(drainContext.coordinator.encodeActorRef(node.system))
        }

        //通知 player sharding
        node.playerSharding.tell(
            requestBuilder.build(),
            self,
        )
    }

    companion object {
        val MaxIdleDuration_1MIN = 1.minutes

        fun props(node: GateNode, session: GatewaySession): Props =
            Props.create(ChannelActor::class.java, node, session)
    }
}
