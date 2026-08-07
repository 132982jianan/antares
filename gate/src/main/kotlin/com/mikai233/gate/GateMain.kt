package com.mikai233.gate

import com.beust.jcommander.JCommander
import com.mikai233.common.conf.RuntimeEnv
import com.mikai233.common.runtime.support.awaitTermination
import com.mikai233.gate.node.Cli
import com.mikai233.gate.node.GateNode
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

    val gateNode = GateNode(addr, cli.name, cli.nodeId ?: "gate-${cli.port}", config, cli.zookeeper, runtimeEnv = runtimeEnv)
    gateNode.launch()
    gateNode.awaitTermination()
}
