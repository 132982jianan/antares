package com.mikai233.common.runtime.support

import com.mikai233.common.broadcast.PlayerBroadcastEventBus
import com.mikai233.common.config.*
import com.mikai233.common.db.MongoDB
import com.mikai233.common.runtime.LocalEntityRegistry
import com.mikai233.common.runtime.WorldRuntimeStateStore
import com.mikai233.common.runtime.module.*
import com.mikai233.common.time.GameTimeOverrideStore
import com.mikai233.common.time.GameTimeSource
import io.github.realmlabs.asteria.cluster.pekko.EntityShardRegistry
import io.github.realmlabs.asteria.cluster.pekko.SingletonActorRegistry
import io.github.realmlabs.asteria.config.ConfigService
import io.github.realmlabs.asteria.config.ConfigSnapshot
import io.github.realmlabs.asteria.core.*
import io.github.realmlabs.asteria.patch.PatchableServiceRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.await
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.actor.ActorSystem


//////////////////////////get/////////////////////////////

val NodeRuntime.system: ActorSystem
    get() = services.get(ActorSystem::class)

val NodeRuntime.coroutineScope: CoroutineScope
    get() = services.get(CoroutineScope::class)

val NodeRuntime.mongoDB: MongoDB
    get() = services.get(MongoDB::class)

val NodeRuntime.gameTimeSource: GameTimeSource
    get() = services.get(GameTimeSource::class)

val NodeRuntime.gameTimeOverrideStore: GameTimeOverrideStore
    get() = services.get(GameTimeOverrideStore::class)

val NodeRuntime.localEntityRegistry: LocalEntityRegistry
    get() = services.get(LocalEntityRegistry::class)

val NodeRuntime.worldRuntimeStateStore: WorldRuntimeStateStore
    get() = services.get(WorldRuntimeStateStore::class)

val NodeRuntime.gameWorldIds: Set<Long>
    get() = gameWorldConfigService.worldIds

val NodeRuntime.gameWorldConfigs: Map<Long, GameWorldConfig>
    get() = gameWorldConfigService.worldsById

val NodeRuntime.gameConfigSnapshot: ConfigSnapshot
    get() = services.get(ConfigService::class).current()

val NodeRuntime.broadcastRouter: ActorRef
    get() = services.get(PlayerBroadcastRuntime::class).router

val NodeRuntime.playerBroadcastEventBus: PlayerBroadcastEventBus
    get() = services.get(PlayerBroadcastEventBus::class)

val NodeRuntime.patchableServices: PatchableServiceRegistry
    get() = services.get(PatchableServiceRegistry::class)

private val NodeRuntime.gameWorldConfigService: GameWorldConfigService
    get() = services.get(GameWorldConfigService::class)


//////////////////////////方法/////////////////////////////
suspend fun NodeRuntime.awaitTermination() {
    system.getWhenTerminated().await()
}

fun NodeRuntime.entityShard(kind: String): ActorRef {
    return services.get(EntityShardRegistry::class)[EntityKind(kind)]
}

fun NodeRuntime.singletonActor(name: String): ActorRef {
    return services.get(SingletonActorRegistry::class)[SingletonName(name)]
}
