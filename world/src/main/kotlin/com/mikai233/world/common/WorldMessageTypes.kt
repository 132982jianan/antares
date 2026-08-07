package com.mikai233.world.common

import com.mikai233.world.actor.WorldActor
import io.github.realmlabs.asteria.message.ActorHandlerContext
import io.github.realmlabs.asteria.message.MessageHandler
import io.github.realmlabs.asteria.message.PatchableMessageHandlerRegistry

typealias WorldHandlerContext = ActorHandlerContext<WorldActor>
typealias WorldMessageHandler<M> = MessageHandler<WorldHandlerContext, M>
typealias WorldMessageHandlerRegistry<M> = PatchableMessageHandlerRegistry<WorldHandlerContext, M>
