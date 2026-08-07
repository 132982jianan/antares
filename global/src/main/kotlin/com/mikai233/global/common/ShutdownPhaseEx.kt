package com.mikai233.global.common

import com.mikai233.protocol.ProtoRpcShutdown.ShutdownPhase

fun ShutdownPhase.isTerminal(): Boolean {
    return this in setOf(
        ShutdownPhase.SHUTDOWN_PHASE_IDLE,
        ShutdownPhase.SHUTDOWN_PHASE_COMPLETED,
        ShutdownPhase.SHUTDOWN_PHASE_FAILED,
    )
}
