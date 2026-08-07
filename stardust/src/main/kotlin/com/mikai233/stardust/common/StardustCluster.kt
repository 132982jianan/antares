package com.mikai233.stardust.common

import java.net.InetSocketAddress

object StardustCluster {
    private typealias NodeFactory = (
        addr: InetSocketAddress,
        name: String,
        nodeId: String,
        config: Config,
        zookeeperConnectString: String,
        sameJvm: Boolean,
        runtimeEnv: RuntimeEnv,
    ) -> LaunchableNode

    private val logger = logger()
    private val nodeByRole: Map<String, NodeFactory> = mapOf(
        GameRoles.Player to { addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv ->
            PlayerNode(addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv)
        },
        GameRoles.Gate to { addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv ->
            GateNode(addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv)
        },
        GameRoles.World to { addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv ->
            WorldNode(addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv)
        },
        GameRoles.Global to { addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv ->
            GlobalNode(addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv)
        },
        GameRoles.Gm to { addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv ->
            GmNode(addr, name, nodeId, config, zookeeperConnectString, sameJvm, runtimeEnv)
        },
    )

    suspend fun launch() {
        val runtimeEnv = RuntimeEnv.fromSystem()
        val repository = RuntimeConfigRepository(
            ZookeeperConfigStore(asyncZookeeperClient(runtimeEnv.zookeeperConnect)),
            JacksonConfigCodec(),
        )
        val layout = ClusterConfigLayout.default(SYSTEM_NAME)
        val nodeConfigs = repository.children<RuntimeNodeConfig>(layout.nodes)
            .values
            .values
            .map { it.value }
            .sortedByDescending { it.seed }
        check(nodeConfigs.isNotEmpty()) {
            "no runtime node configs found under ${layout.nodes}; run tools.zookeeper.ZookeeperInitializer first"
        }

        val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
            logger.error("failed to launch development cluster", throwable)
            (LoggerFactory.getILoggerFactory() as LoggerContext).stop()
            exitProcess(-1)
        }

        val nodes: List<LaunchableNode> = supervisorScope {
            nodeConfigs.map { nodeConfig ->
                async(exceptionHandler) {
                    logger.info("launch development node: {}", nodeConfig)
                    val role = requireNotNull(nodeConfig.roles.firstOrNull(nodeByRole::containsKey)) {
                        "node ${nodeConfig.nodeId} has no known game role: ${nodeConfig.roles}"
                    }
                    val nodeFactory = requireNotNull(nodeByRole[role]) { "node factory missing for role: $role" }
                    val addr = InetSocketAddress(nodeConfig.host, nodeConfig.port)
                    val config = ConfigFactory.load("${role.lowercase()}.conf")
                    nodeFactory(
                        addr,
                        SYSTEM_NAME,
                        nodeConfig.nodeId,
                        config,
                        runtimeEnv.zookeeperConnect,
                        true,
                        runtimeEnv,
                    ).also { it.launch() }
                }
            }.awaitAll()
        }
        supervisorScope {
            nodes.forEach { node ->
                launch(exceptionHandler) {
                    node.services.get(ActorSystem::class).whenTerminated.await()
                }
            }
        }
    }
}
