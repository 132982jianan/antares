package com.mikai233.common.runtime.support

import io.github.realmlabs.asteria.core.NodeRuntime

interface LaunchableNode : NodeRuntime {
    suspend fun launch()
}
