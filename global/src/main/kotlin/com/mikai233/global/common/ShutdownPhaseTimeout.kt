package com.mikai233.global.common

import com.mikai233.protocol.ProtoRpcShutdown

data class ShutdownPhaseTimeout(
    val planId: String,
    val generation: Long,
    val phase: ProtoRpcShutdown.ShutdownPhase,
)
