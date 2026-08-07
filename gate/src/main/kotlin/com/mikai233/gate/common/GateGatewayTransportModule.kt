package com.mikai233.gate.common

import com.mikai233.common.config.NettyConfig
import com.mikai233.common.config.nettyConfigPath
import com.mikai233.gate.node.GateNode
import io.github.realmlabs.asteria.config.center.RuntimeConfigRepository
import io.github.realmlabs.asteria.core.AsteriaModule
import io.github.realmlabs.asteria.core.ModuleContext
import io.github.realmlabs.asteria.gateway.netty.NettyGatewayServerOptions
import io.github.realmlabs.asteria.gateway.netty.NettyTcpGatewayServerTransport
import io.github.realmlabs.asteria.observability.Metrics
import io.github.realmlabs.asteria.observability.NoopMetrics
import kotlinx.coroutines.CoroutineScope
import org.apache.pekko.actor.ActorSystem

class GateGatewayTransportModule(
    private val node: GateNode,
) : AsteriaModule {
    override val name: String = "gate-gateway-transport"

    private var transport: NettyTcpGatewayServerTransport? = null

    override suspend fun start(context: ModuleContext) {
        //创建 GateShutdownListenerActor ，监听分布式 PubSub 的 GATE_DRAIN_TOPIC 主题，用于协调停机
        context.services.get(ActorSystem::class)
            .actorOf(GateShutdownListenerActor.props(node), GateShutdownListenerActor.Name)

        val repository = context.services.get(RuntimeConfigRepository::class)

        //从 ZooKeeper 读取 Netty 配置（host/port）
        val config = repository.get<NettyConfig>(nettyConfigPath(node.nodeId))?.value
            ?: repository.get<NettyConfig>(nettyConfigPath("gate"))?.value
            ?: error("runtime config ${nettyConfigPath(node.nodeId)} or ${nettyConfigPath("gate")} not found")

        //启动 Netty TCP 网关服务器 ，使用自定义管道：
        val gatewayTransport = NettyTcpGatewayServerTransport(
            NettyGatewayServerOptions(
                host = config.host,
                port = config.port,
                maxFrameLength = 1024 * 100,
            ),
            scope = context.services.get(CoroutineScope::class),
            metrics = context.services.find(Metrics::class) ?: NoopMetrics,
            pipelineInstaller = GateNettyPipeline.installer(node.protocolCodec),
        )
        //连接处理器
        gatewayTransport.start(GateTransportHandler(node))

        transport = gatewayTransport
    }

    override suspend fun stop(context: ModuleContext) {
        node.connectionDrainer.beginDrain("gateway transport stopping")
        transport?.stop()
        node.connectionDrainer.closeAll()
        transport = null
    }
}
