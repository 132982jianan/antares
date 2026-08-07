package com.mikai233.gate.node

import com.beust.jcommander.Parameter
import com.mikai233.common.conf.RuntimeEnv
import com.mikai233.common.config.SYSTEM_NAME
import com.mikai233.common.rpc.DefaultRpcEntityIdResolver
import com.mikai233.common.rpc.GameRpcProtocol
import com.mikai233.common.rpc.RpcEntityIdResolver
import com.mikai233.common.runtime.*
import com.mikai233.gate.common.GamePatchBindings
import com.mikai233.gate.common.GateConnectionDrainer
import com.mikai233.gate.common.GateGatewayRouter
import com.mikai233.gate.common.GateGatewayTransportModule
import com.mikai233.gate.common.GateProtocolCodec
import com.mikai233.gate.generated.GeneratedGateNodeDispatchers
import com.typesafe.config.Config
import io.github.realmlabs.asteria.cluster.pekko.extractor
import io.github.realmlabs.asteria.core.NodeState
import io.github.realmlabs.asteria.core.RoleKey
import io.github.realmlabs.asteria.core.ServiceRegistry
import io.github.realmlabs.asteria.patch.PatchableServiceRegistry
import org.apache.pekko.actor.ActorRef
import java.net.InetSocketAddress

/*
这个文件是 Antares 游戏服务器的 Gate（网关）节点 的核心启动类。它是整个分布式游戏架构中客户端的入口节点。下面逐段分析。

GateNode 实现了 LaunchableNode 接口，这意味着它是一个可启动的集群节点。
 */
class GateNode(
    //节点绑定的地址，默认 2334 端口
    val addr: InetSocketAddress,
    override val name: String,
    //集群中的唯一标识，格式为 gate-{port}
    val nodeId: String = "gate-${addr.port}",
    val config: Config,
    //ZooKeeper 连接串，用于集群服务发现和配置管理
    zookeeperConnectString: String,
    //是否在同一个 JVM 进程内运行多个节点（开发/测试模式）
    sameJvm: Boolean = false,
    //运行时环境变量（如机器 IP）
    val runtimeEnv: RuntimeEnv = RuntimeEnv.fromSystem(),
) : LaunchableNode {
    //声明本节点在集群中的角色为 Gate 。Asteria 框架会用角色来决定哪些节点承载哪些实体/单例 Acto
    override val roles: Set<RoleKey> = setOf(RoleKey(GameRoles.Gate))

    //服务注册表，基于 Asteria 框架的 DI 容器
    override val services: ServiceRegistry = ServiceRegistry()

    //状态管理 ：使用 @Volatile 保证多线程可见性，初始状态为 Unstarted
    @Volatile
    private var currentState: NodeState = NodeState.Unstarted

    override val state: NodeState
        get() = currentState

    //是整个集群启动的核心助手，封装了 Pekko 集群的初始化、角色注册、实体分片（Sharding）配置等。
    private val clusterNode = ClusterNodeBootstrap(this, addr, nodeId, config, zookeeperConnectString, sameJvm)

    //ShardRegion 代理 ：用于向玩家实体（PlayerActor）发送消息的路由器. Gate 节点 不承载 PlayerActor/WorldActor 实例本身（注意 launch() 中没有定义 Actor 实现），而是作为 代理/客户端 把请求转发到承载对应角色的节点上。
    val playerSharding: ActorRef
        get() = entityShard(GameEntityKinds.PlayerActor)

    //ShardRegion 代理 ：用于向世界实体（WorldActor）发送消息的路由器. Gate 节点 不承载 PlayerActor/WorldActor 实例本身（注意 launch() 中没有定义 Actor 实现），而是作为 代理/客户端 把请求转发到承载对应角色的节点上。
    val worldSharding: ActorRef
        get() = entityShard(GameEntityKinds.WorldActor)

    //代码生成的消息分发器，将 Gate 节点的 Protobuf 消息类型映射到对应的处理器
    val protobufDispatcher = GeneratedGateNodeDispatchers.PROTOBUF

    //在 Asteria 的 GatewayFrame 和游戏自定义的 Protobuf 消息格式之间做编解码适配。格式是 [4字节消息ID + Protobuf序列化数据]
    val protocolCodec = GateProtocolCodec()

    //网关消息路由器。使用 by lazy 延迟初始化。它根据消息类型决定路由目标：
    //  - 玩家相关消息 → PlayerActor 分片
    //  - 世界相关消息 → WorldActor 分片
    //  - GM 命令 → 按命令映射到对应目标
    //  - 本地消息 → 直接交给 ChannelActor 处理
    val gatewayRouter: GateGatewayRouter by lazy { GateGatewayRouter(this) }

    //连接排空器，用于优雅停机。当 Gate 节点准备下线时，拒绝新连接并逐步关闭已有连接
    val connectionDrainer = GateConnectionDrainer()

    /*
    服务注册
     */
    init {
        val patchableServices = PatchableServiceRegistry().apply {
            register(RpcEntityIdResolver::class, DefaultRpcEntityIdResolver(GameRpcProtocol.protocol))
        }
        services.register(
            GamePatchBindings::class,
            GamePatchBindings(
                services = patchableServices,
                gateMessageRegistry = GeneratedGateNodeDispatchers.PROTOBUF_REGISTRY,
            ),
        )
        services.register(PatchableServiceRegistry::class, patchableServices)
    }

    override suspend fun launch() {
        clusterNode.launch(
            afterClusterModules = listOf(GateGatewayTransportModule(this)),
            onStateChange = ::updateState,
        ) {
            role(GameRoles.Gate)
            entity<Long>(GameEntityKinds.PlayerActor) {
                role(GameRoles.Player)
                shardCount = PLAYER_SHARD_NUM
                extractor(GameRpcProtocol.playerShardExtractor(this@GateNode))
            }
            entity<Long>(GameEntityKinds.WorldActor) {
                role(GameRoles.World)
                shardCount = WORLD_SHARD_NUM
                extractor(GameRpcProtocol.worldShardExtractor(this@GateNode))
            }
        }
    }

    private fun updateState(newState: NodeState) {
        currentState = newState
        if (newState == NodeState.Stopping) {
            connectionDrainer.beginDrain("node stopping")
        }
    }
}

internal class Cli(runtimeEnv: RuntimeEnv) {
    @Parameter(names = ["-h", "--host"], description = "host")
    var host: String = runtimeEnv.machineIp

    @Parameter(names = ["-p", "--port"], description = "port")
    var port: Int = 2334

    @Parameter(names = ["-c", "--conf"], description = "conf")
    var conf: String = "gate.conf"

    @Parameter(names = ["-z", "--zookeeper"], description = "zookeeper")
    var zookeeper: String = runtimeEnv.zookeeperConnect

    @Parameter(names = ["-n", "--name"], description = "system name")
    var name: String = SYSTEM_NAME

    @Parameter(names = ["-i", "--node-id"], description = "runtime node id")
    var nodeId: String? = null
}
