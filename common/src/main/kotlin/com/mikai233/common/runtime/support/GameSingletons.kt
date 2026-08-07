package com.mikai233.common.runtime.support

object GameSingletons {
    const val Worker = "worker"
    const val Monitor = "monitor"
    const val ShutdownCoordinator = "shutdownCoordinator"

    val all: List<String> = listOf(Worker, Monitor, ShutdownCoordinator)
}
