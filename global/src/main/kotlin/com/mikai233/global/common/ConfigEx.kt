package com.mikai233.global.common

import com.typesafe.config.Config
import java.time.Duration

fun Config.getDurationOrDefault(path: String, defaultValue: Duration): Duration {
    return if (hasPath(path)) getDuration(path) else defaultValue
}
