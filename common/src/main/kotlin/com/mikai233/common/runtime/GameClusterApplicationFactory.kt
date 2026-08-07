package com.mikai233.common.runtime

//项目内部的协程作用域模块，用于与 Pekko Actor 系统集成。
import com.mikai233.common.runtime.module.PekkoCoroutineScopeModule
//Typesafe 配置库，用于读取和管理 HOCON 格式配置
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

//Asteria 框架的核心类
//AsteriaApplication是应用入口
import io.github.realmlabs.asteria.core.AsteriaApplication
//AsteriaApplicationBuilder 是构建器
import io.github.realmlabs.asteria.core.AsteriaApplicationBuilder
//AsteriaModule 是模块基类
import io.github.realmlabs.asteria.core.AsteriaModule
//NodeRuntime 表示节点运行时环境
import io.github.realmlabs.asteria.core.NodeRuntime
//Asteria 提供的 Pekko 补丁控制模块
import io.github.realmlabs.asteria.patch.pekko.PekkoPatchControlModule
//脚本引擎，支持 Groovy 和 Jar 格式的脚本
import io.github.realmlabs.asteria.script.engine.groovy.GroovyScriptEngine
import io.github.realmlabs.asteria.script.engine.jar.JarScriptEngine
//Asteria 的脚本模块，用于管理脚本引擎
import io.github.realmlabs.asteria.script.pekko.ScriptModule
//Java 标准库的 IP + 端口地址类
import java.net.InetSocketAddress

/*
这是一个 internal （模块内可见）数据类，封装了构建游戏集群应用所需的所有参数。
    - NodeRuntime ：封装了节点运行时的元信息（如名称、环境等）。
    - InetSocketAddress ：保存 IP 地址和端口，表示该节点的网络定位。
    - sameJvm ：当多个 Pekko 集群节点运行在同一 JVM 时，需要特殊设置 JMX MBean。
    - beforeClusterModules / afterClusterModules ：允许在集群加入前后安装不同模块，控制初始化顺序。
    - configure ：一个 Kotlin 扩展函数类型的 lambda，允许调用者自定义 AsteriaApplicationBuilder 的配置。
 */
internal data class GameClusterApplicationRequest(
    // 节点运行时
    val runtime: NodeRuntime,
    // 节点网络地址
    val addr: InetSocketAddress,
    // 节点唯一 ID
    val nodeId: String,
    // 配置对象
    val config: Config,
    // 是否同一 JVM 上运行多个节点
    val sameJvm: Boolean,
    // 通用模块列表
    val commonModules: List<AsteriaModule>,
    // 集群初始化前安装的模块
    val beforeClusterModules: List<AsteriaModule>,
    // 集群初始化后安装的模块
    val afterClusterModules: List<AsteriaModule>,
    // 自定义配置 lambda
    val configure: AsteriaApplicationBuilder.() -> Unit,
) {
    /*
    如果 sameJvm 为 true ，则在原始配置之上追加 pekko.cluster.jmx.multi-mbeans-in-same-jvm = on （允许多个 Pekko ActorSystem 在同 JVM 中注册各自的 JMX MBean）。
        否则直接返回原配置。
         withFallback 表示新配置优先，原始配置作为后备。
     */
    fun runtimeConfig(): Config {
        return if (sameJvm) {
            ConfigFactory
                .parseMap(mapOf("pekko.cluster.jmx.multi-mbeans-in-same-jvm" to "on"))
                .withFallback(config)
        } else {
            config
        }
    }
}

internal interface GameClusterApplicationFactory {
    fun build(request: GameClusterApplicationRequest): AsteriaApplication
}

internal object GameClusterApplicationFactories {
    fun select(config: Config): GameClusterApplicationFactory {
        val configuredMode = if (config.hasPath("game.cluster.discovery")) {
            config.getString("game.cluster.discovery")
        } else {
            null
        }
        val raw = System.getenv("CLUSTER_DISCOVERY") ?: configuredMode ?: "config-center"
        return when (raw.lowercase()) {
            "kubernetes", "k8s" -> KubernetesGameClusterApplicationFactory
            "config-center", "zookeeper", "zk", "topology" -> ConfigCenterGameClusterApplicationFactory
            else -> error("Unsupported cluster discovery mode: $raw")
        }
    }
}

internal fun AsteriaApplicationBuilder.installGameNodeModules(request: GameClusterApplicationRequest) {
    name = request.runtime.name
    request.commonModules.forEach(::install)
    request.beforeClusterModules.forEach(::install)
    request.configure(this)
    install(PekkoCoroutineScopeModule())
    install(
        ScriptModule {
            engine(GroovyScriptEngine())
            engine(JarScriptEngine())
            allowNodeScripts = true
            allowActorScripts = true
        },
    )
    install(PekkoPatchControlModule())
    request.afterClusterModules.forEach(::install)
}
