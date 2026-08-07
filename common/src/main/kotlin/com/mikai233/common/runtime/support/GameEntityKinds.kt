package com.mikai233.common.runtime.support

object GameEntityKinds {
    const val PlayerActor = "PlayerActor"
    const val WorldActor = "WorldActor"

    val all: List<String> = listOf(PlayerActor, WorldActor)
}
