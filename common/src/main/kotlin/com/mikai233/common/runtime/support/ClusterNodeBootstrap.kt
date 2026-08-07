package com.mikai233.common.runtime.support

// 配置相关常量（如 WORKER_IDS, PATCH_ARTIFACTS 等）
import com.mikai233.common.config.*
// ZK 客户端扩展函数
import com.mikai233.common.extension.asyncZookeeperClient
// 游戏集群应用工厂选择器
import com.mikai233.common.runtime.GameClusterApplicationFactories
// 构建应用的请求参数
import com.mikai233.common.runtime.GameClusterApplicationRequest
// 通用模块（Prometheus, Mongo, GameTime, GameConfig 等）
import com.mikai233.common.runtime.module.*
// 热补丁/补丁相关模块
import com.mikai233.common.runtime.patch.ConfigCenterPatchArtifactStore
import com.mikai233.common.runtime.patch.ConfigCenterRuntimePatchRepository
import com.mikai233.common.runtime.patch.GamePatchStoreModule
// 运行时版本号
import com.mikai233.common.runtime.runtimeVersion
// HOCON 配置对象
import com.typesafe.config.Config
// Asteria 框架核心（AsteriaModule, AsteriaApplicationBuilder, NodeState, NodeRuntime）
// Pekko 集群的协程任务扩展
import io.github.realmlabs.asteria.cluster.pekko.addSuspendTask

// ZK 配置中心模块
import io.github.realmlabs.asteria.config.center.zookeeper.ZookeeperConfigCenterModule
import io.github.realmlabs.asteria.config.center.zookeeper.ZookeeperConfigStore
import io.github.realmlabs.asteria.core.*

// WorkerId 分配模块（雪花算法机器号）
import io.github.realmlabs.asteria.id.WorkerIdModule
import io.github.realmlabs.asteria.id.WorkerIdModuleOptions
import io.github.realmlabs.asteria.id.WorkerIdOwner

// 基于 ZK 的 WorkerId 仓库
import io.github.realmlabs.asteria.id.zookeeper.ZookeeperWorkerIdRepository

// 补丁框架核心
import io.github.realmlabs.asteria.patch.PatchModule

// Jar 包补丁解析器
import io.github.realmlabs.asteria.patch.jar.JarRuntimePatchPluginResolver

// Pekko 环境下的补丁环境提供者
import io.github.realmlabs.asteria.patch.pekko.PekkoPatchEnvironmentProvider
import kotlinx.coroutines.delay

// Curator 异步 ZK 客户端
import org.apache.curator.x.async.AsyncCuratorFramework
// Pekko 协调关闭机制
import org.apache.pekko.actor.CoordinatedShutdown
import org.slf4j.LoggerFactory
// IP+端口地址
import java.net.InetSocketAddress
import kotlin.random.Random
// 随机 Long 扩展
import kotlin.random.nextLong
// 毫秒时间单位
import kotlin.time.Duration.Companion.milliseconds

/*
总结:
  ClusterNodeBootstrap 是 集群节点的统一启动器 ，负责：
    1. 初始化 ZK 连接
    2. 组装所有通用模块（配置中心、热补丁、Mongo、Metrics、游戏相关等）
    3. 协调 Asteria 应用生命周期（工厂模式构建 → 绑定 runtime → 启动）
    4. 注册 Pekko 的优雅关闭钩子（延迟离群 + 状态通知）

Gate 和 Game 节点都通过传入不同的 beforeClusterModules / afterClusterModules / configure DSL 来实现差异化启动。
 */
class ClusterNodeBootstrap(
    // 节点运行时（生命周期状态、服务注册等）
    private val runtime: NodeRuntime,
    // 当前节点监听的 IP 地址和端口
    private val addr: InetSocketAddress,
    // 节点唯一标识
    private val nodeId: String,
    // Typesafe/HOCON 配置
    private val config: Config,
    // ZooKeeper 连接串（如 "zk1:2181,zk2:2181"）
    private val zookeeperConnectString: String,
    // 是否同一 JVM（多节点本地测试用）
    private val sameJvm: Boolean = false,
) {
    private val logger = LoggerFactory.getLogger(runtime::class.java)

    //懒初始化异步 Curator 客户端。 asyncZookeeperClient 是项目自定义的扩展函数，负责创建并配置 ZK 连接。
    private val zookeeperClient: AsyncCuratorFramework by lazy {
        asyncZookeeperClient(zookeeperConnectString)
    }

    //对外暴露 ZK 客户端：优先从 runtime.services （服务注册表）中查找已有的实例，找不到才用懒加载的那个。这样同一 JVM 多节点场景可以共享同一个 ZK 连接。
    val zookeeper: AsyncCuratorFramework
        get() = runtime.services.find(AsyncCuratorFramework::class) ?: zookeeperClient

    /*
    launch 是核心启动入口， suspend 是因为涉及 ZK 等异步操作
     */
    suspend fun launch(
        // 集群启动前要加载的模块
        beforeClusterModules: List<AsteriaModule> = emptyList(),
        // 集群启动后要加载的模块（如 Gate 的 TCP 网关模块）
        afterClusterModules: List<AsteriaModule> = emptyList(),
        // 状态变化回调（通知外部当前节点状态）
        onStateChange: (NodeState) -> Unit,
        // DSL 配置块（注册角色、实体分片等）
        configure: AsteriaApplicationBuilder.() -> Unit,
    ) {
        //构建并启动应用-通过工厂模式选择一个 GameClusterApplicationFactory ，用 GameClusterApplicationRequest 封装所有参数，构建出 AsteriaApplication 。
        val application = GameClusterApplicationFactories
            .select(config)
            .build(
                GameClusterApplicationRequest(
                    runtime = runtime,
                    addr = addr,
                    nodeId = nodeId,
                    config = config,
                    sameJvm = sameJvm,

                    //加载通用模块 (commonModules)
                    commonModules = commonModules(),

                    // 集群前模块 加载 beforeClusterModules（Gate 无）
                    beforeClusterModules = beforeClusterModules,

                    //集群后模块 + 时间重载模块 加载 afterClusterModules → GateGatewayTransportModule（启动 Netty TCP 网关）
                    afterClusterModules = afterClusterModules + GameTimeReloadModule(nodeId),

                    // 执行 DSL 配置块：
                    //   - 注册 Gate 角色
                    //   - 注册 PlayerActor 实体分片（3000 个分片，但只做代理）
                    //   - 注册 WorldActor 实体分片（3000 个分片，但只做代理）
                    configure = configure,
                ),
            )

        //将应用绑定到 runtime ，并监听节点状态变更（ Starting → Running → Stopping → Stopped ），既通知外部回调也打印日志。
        val lifecycle = application.bind(runtime) { newState ->
            val previousState = runtime.state
            onStateChange(newState)
            logger.info("{} state change from:{} to:{}", runtime::class.simpleName, previousState, newState)
        }

        //真正启动应用生命周期。
        lifecycle.launch()

        //注册 Pekko 协调关闭的钩子任务。
        addCoordinatedShutdownTasks(onStateChange)
    }

    /*
    创建 WorkerId 模块 （雪花算法的机器号分配）。基于 ZK 存储，用当前节点地址作为 owner 标识。节点启动时从 ZK 抢占一个 workerId。
     */
    fun workerIdModule(): AsteriaModule {
        return WorkerIdModule(
            repository = ZookeeperWorkerIdRepository(zookeeper, WORKER_IDS),
            options = WorkerIdModuleOptions(
                owner = { WorkerIdOwner(addr.toString()) },
            ),
        )
    }

    /*
    通用模块列表
     */
    private fun commonModules(): List<AsteriaModule> {
        val patchStore = ZookeeperConfigStore(zookeeper)
        val patchArtifacts = ConfigCenterPatchArtifactStore(patchStore, PATCH_ARTIFACTS)

        return listOf(
            //暴露 Prometheus 指标 HTTP 端点（端口 = 节点端口 + 1000）
            PrometheusMetricsModule(addr.port + 1000),
            //ZK 配置中心，管理运行时常量/开关
            ZookeeperConfigCenterModule {
                client(zookeeper)
            },
            //游戏时间模块（可能用于时间加速/减速/暂停）
            GameTimeModule(config),
            //本地实体注册表
            LocalEntityRegistryModule(),
            //补丁制品存储（从 ZK 拉取 JAR 包）
            GamePatchStoreModule(patchArtifacts),
            //运行时热补丁框架：Pekko 环境 + ZK 配置中心的补丁描述符 + Jar 解析器
            PatchModule {
                environment(PekkoPatchEnvironmentProvider(runtimeVersion()))
                repository(ConfigCenterRuntimePatchRepository(patchStore, PATCH_DESCRIPTORS, PATCH_REVISION))
                resolver(JarRuntimePatchPluginResolver(patchArtifacts))
            },
            //MongoDB 连接模块
            MongoDbModule(),
            //游戏世界配置（场景/地图配置等）
            GameWorldConfigModule(),
            //游戏世界运行时状态
            WorldRuntimeStateModule(),
            //游戏通用配置
            GameConfigModule(),
            //玩家广播模块（跨节点消息广播）
            PlayerBroadcastModule(),
        )
    }

    /*
    优雅关闭
     */
    private fun addCoordinatedShutdownTasks(onStateChange: (NodeState) -> Unit) {
        //获取 Pekko 的协调关闭器，注册三个阶段的钩子：
        with(CoordinatedShutdown.get(runtime.system)) {
            //离开集群
            //  从集群离开前随机延迟 1-5 秒—— 防止所有节点同时离开集群造成脑裂 （经典的"惊群效应"规避手段)
            addSuspendTask(
                CoordinatedShutdown.PhaseClusterLeave(),
                "leave_delay",
            ) {
                delay(Random.nextLong(1000L..5000L).milliseconds)
            }

            //解绑服务
            //  在解除服务绑定之前，通知外部状态变为 Stopping （例如从负载均衡中摘除）
            addSuspendTask(
                CoordinatedShutdown.PhaseBeforeServiceUnbind(),
                "change_state_stopping",
            ) {
                onStateChange(NodeState.Stopping)
            }

            //Actor 系统终止
            //  Actor 系统终止时，通知外部状态变为 Stopped
            addSuspendTask(
                CoordinatedShutdown.PhaseActorSystemTerminate(),
                "change_state_stopped",
            ) {
                onStateChange(NodeState.Stopped)
            }
        }
    }
}
