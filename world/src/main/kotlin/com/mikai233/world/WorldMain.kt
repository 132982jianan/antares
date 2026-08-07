package com.mikai233.world

import com.beust.jcommander.JCommander
import com.mikai233.common.conf.RuntimeEnv
import com.mikai233.common.runtime.awaitTermination
import com.mikai233.world.node.Cli
import com.mikai233.world.node.WorldNode
import com.typesafe.config.ConfigFactory
import java.net.InetSocketAddress

suspend fun main(args: Array<String>) {
    val runtimeEnv = RuntimeEnv.fromSystem()
    val cli = Cli(runtimeEnv)
    @Suppress("SpreadOperator")
    JCommander.newBuilder()
        .addObject(cli)
        .build()
        .parse(*args)
    val addr = InetSocketAddress(cli.host, cli.port)
    val config = ConfigFactory.load(cli.conf)
    WorldNode(addr, cli.name, cli.nodeId ?: "world-${cli.port}", config, cli.zookeeper, runtimeEnv = runtimeEnv).also {
        it.launch()
        it.awaitTermination()
    }
}
