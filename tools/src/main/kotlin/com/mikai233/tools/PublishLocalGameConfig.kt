package com.mikai233.tools

import com.mikai233.common.conf.RuntimeEnv
import com.mikai233.common.extension.asyncZookeeperClient
import com.mikai233.tools.config.LocalGameConfigPublisher
import io.github.realmlabs.asteria.config.center.zookeeper.ZookeeperConfigStore
import kotlinx.coroutines.runBlocking

suspend fun main() {
    val zookeeperConnect = RuntimeEnv.fromSystem().zookeeperConnect
    LocalGameConfigPublisher.publish(
        ZookeeperConfigStore(asyncZookeeperClient(zookeeperConnect)),
    )
}
