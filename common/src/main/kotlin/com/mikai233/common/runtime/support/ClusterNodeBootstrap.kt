package com.mikai233.common.runtime.support

import com.mikai233.common.config.*
import com.mikai233.common.extension.asyncZookeeperClient
import com.mikai233.common.runtime.GameClusterApplicationFactories
import com.mikai233.common.runtime.GameClusterApplicationRequest
import com.mikai233.common.runtime.module.*
import com.mikai233.common.runtime.patch.ConfigCenterPatchArtifactStore
import com.mikai233.common.runtime.patch.ConfigCenterRuntimePatchRepository
import com.mikai233.common.runtime.patch.GamePatchStoreModule
import com.mikai233.common.runtime.runtimeVersion
import com.typesafe.config.Config
import io.github.realmlabs.asteria.cluster.pekko.addSuspendTask
import io.github.realmlabs.asteria.config.center.zookeeper.ZookeeperConfigCenterModule
import io.github.realmlabs.asteria.config.center.zookeeper.ZookeeperConfigStore
import io.github.realmlabs.asteria.core.*
import io.github.realmlabs.asteria.id.WorkerIdModule
import io.github.realmlabs.asteria.id.WorkerIdModuleOptions
import io.github.realmlabs.asteria.id.WorkerIdOwner
import io.github.realmlabs.asteria.id.zookeeper.ZookeeperWorkerIdRepository
import io.github.realmlabs.asteria.patch.PatchModule
import io.github.realmlabs.asteria.patch.jar.JarRuntimePatchPluginResolver
import io.github.realmlabs.asteria.patch.pekko.PekkoPatchEnvironmentProvider
import kotlinx.coroutines.delay
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.CoordinatedShutdown
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import kotlin.random.Random
import kotlin.random.nextLong
import kotlin.time.Duration.Companion.milliseconds

class ClusterNodeBootstrap(
    private val runtime: NodeRuntime,
    private val addr: InetSocketAddress,
    private val nodeId: String,
    private val config: Config,
    private val zookeeperConnectString: String,
    private val sameJvm: Boolean = false,
) {
    private val logger = LoggerFactory.getLogger(runtime::class.java)

    private val zookeeperClient: AsyncCuratorFramework by lazy {
        asyncZookeeperClient(zookeeperConnectString)
    }

    val zookeeper: AsyncCuratorFramework
        get() = runtime.services.find(AsyncCuratorFramework::class) ?: zookeeperClient

    suspend fun launch(
        beforeClusterModules: List<AsteriaModule> = emptyList(),
        afterClusterModules: List<AsteriaModule> = emptyList(),
        onStateChange: (NodeState) -> Unit,

        // DSL模块
        configure: AsteriaApplicationBuilder.() -> Unit,
    ) {
        val application = GameClusterApplicationFactories.select(config).build(
            GameClusterApplicationRequest(
                runtime = runtime,
                addr = addr,
                nodeId = nodeId,
                config = config,
                sameJvm = sameJvm,

                //加载通用模块 (commonModules)
                commonModules = commonModules(),

                //加载 beforeClusterModules（Gate 无）
                beforeClusterModules = beforeClusterModules,

                //加载 afterClusterModules → GateGatewayTransportModule（启动 Netty TCP 网关）
                afterClusterModules = afterClusterModules + GameTimeReloadModule(nodeId),

                // 执行 DSL 配置块：
                //   - 注册 Gate 角色
                //   - 注册 PlayerActor 实体分片（3000 个分片，但只做代理）
                //   - 注册 WorldActor 实体分片（3000 个分片，但只做代理）
                configure = configure,
            ),
        )
        val lifecycle = application.bind(runtime) { newState ->
            val previousState = runtime.state
            onStateChange(newState)
            logger.info("{} state change from:{} to:{}", runtime::class.simpleName, previousState, newState)
        }
        lifecycle.launch()
        addCoordinatedShutdownTasks(onStateChange)
    }

    fun workerIdModule(): AsteriaModule {
        return WorkerIdModule(
            repository = ZookeeperWorkerIdRepository(zookeeper, WORKER_IDS),
            options = WorkerIdModuleOptions(
                owner = { WorkerIdOwner(addr.toString()) },
            ),
        )
    }

    private fun commonModules(): List<AsteriaModule> {
        val patchStore = ZookeeperConfigStore(zookeeper)
        val patchArtifacts = ConfigCenterPatchArtifactStore(patchStore, PATCH_ARTIFACTS)
        return listOf(
            PrometheusMetricsModule(addr.port + 1000),
            ZookeeperConfigCenterModule {
                client(zookeeper)
            },
            GameTimeModule(config),
            LocalEntityRegistryModule(),
            GamePatchStoreModule(patchArtifacts),
            PatchModule {
                environment(PekkoPatchEnvironmentProvider(runtimeVersion()))
                repository(ConfigCenterRuntimePatchRepository(patchStore, PATCH_DESCRIPTORS, PATCH_REVISION))
                resolver(JarRuntimePatchPluginResolver(patchArtifacts))
            },
            MongoDbModule(),
            GameWorldConfigModule(),
            WorldRuntimeStateModule(),
            GameConfigModule(),
            PlayerBroadcastModule(),
        )
    }

    private fun addCoordinatedShutdownTasks(onStateChange: (NodeState) -> Unit) {
        with(CoordinatedShutdown.get(runtime.system)) {
            addSuspendTask(
                CoordinatedShutdown.PhaseClusterLeave(),
                "leave_delay",
            ) {
                delay(Random.nextLong(1000L..5000L).milliseconds)
            }
            addSuspendTask(
                CoordinatedShutdown.PhaseBeforeServiceUnbind(),
                "change_state_stopping",
            ) {
                onStateChange(NodeState.Stopping)
            }
            addSuspendTask(
                CoordinatedShutdown.PhaseActorSystemTerminate(),
                "change_state_stopped",
            ) {
                onStateChange(NodeState.Stopped)
            }
        }
    }
}
