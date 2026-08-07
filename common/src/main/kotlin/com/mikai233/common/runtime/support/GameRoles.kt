package com.mikai233.common.runtime.support

object GameRoles {
    const val Player = "Player"
    const val Gate = "Gate"
    const val World = "World"
    const val Global = "Global"
    const val Gm = "Gm"

    val all: List<String> = listOf(Player, Gate, World, Global, Gm)
}
