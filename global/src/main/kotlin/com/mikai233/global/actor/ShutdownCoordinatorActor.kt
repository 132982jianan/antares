package com.mikai233.global.actor

import com.mikai233.common.extension.encodeActorRef
// 角色常量（如 Gate）
import com.mikai233.common.runtime.support.GameRoles
// 获取所有 World ID 的扩展属性
import com.mikai233.common.runtime.support.gameWorldIds
// 获取 ActorSystem 的扩展属性
import com.mikai233.common.runtime.support.system
// Gate 排水的分布式 PubSub 主题名
import com.mikai233.common.shutdown.GATE_DRAIN_TOPIC
import com.mikai233.global.common.ShutdownPhaseTimeout
import com.mikai233.global.common.formatIds
import com.mikai233.global.common.getDurationOrDefault
import com.mikai233.global.common.isTerminal
// Global 节点核心类
import com.mikai233.global.node.GlobalNode
// 移交协调器所有权消息
import com.mikai233.global.message.HandoffShutdownCoordinator
import com.mikai233.protocol.ProtoRpcPlayer.PlayerShutdownAck
// ShutdownStartReq, GateDrainAck 等
import com.mikai233.protocol.ProtoRpcShutdown.*
import com.mikai233.protocol.ProtoRpcWorld.WorldShutdownAck
import com.mikai233.protocol.ProtoRpcWorld.WorldShutdownReq
import io.github.realmlabs.asteria.actor.AsteriaActor
import org.apache.pekko.actor.Props
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.MemberStatus
import org.apache.pekko.cluster.pubsub.DistributedPubSub
import org.apache.pekko.cluster.pubsub.DistributedPubSubMediator.Publish
import java.time.Duration

/*
功能:
    这是一个基于 Pekko Actor 的 优雅停机协调器 ，负责按阶段协调整个集群的关闭流程
    参数: 持有 GlobalNode 引用，用于获取配置、Sharding 等
    继承 AsteriaActor<GlobalNode> ，这是项目自定义的 Actor 基类，封装了日志、调度等通用能力。
 */
class ShutdownCoordinatorActor(val node: GlobalNode) : AsteriaActor<GlobalNode>(node) {
    //获取 Pekko 分布式发布-订阅的 mediator 引用，用于向集群其他节点广播消息
    private val mediator = DistributedPubSub.get(context.system).mediator()

    //Gate 排水 ：默认 30 秒
    private val gateDrainTimeout = node.config.getDurationOrDefault(
        "game.shutdown.timeout.gate-drain",
        Duration.ofSeconds(30),
    )

    //Player 排水 ：默认 2 分钟
    private val playerDrainTimeout = node.config.getDurationOrDefault(
        "game.shutdown.timeout.player-drain",
        Duration.ofMinutes(2),
    )

    //World 停止 ：默认 3 分钟
    private val worldStopTimeout = node.config.getDurationOrDefault(
        "game.shutdown.timeout.world-stop",
        Duration.ofMinutes(3),
    )

    // 当前停机计划的唯一标识
    private var planId: String? = null
    private var requestedBy: String? = null
    private var phase: ShutdownPhase = ShutdownPhase.SHUTDOWN_PHASE_IDLE
    private var generation: Long = 0
    private var expectedGateCount: Int = 0

    // 已排水完成的 Gate 节点 ID 集合（有序）
    private val drainedGateNodes = linkedSetOf<String>()

    // 期望下线的玩家 ID
    private val expectedPlayerIds = linkedSetOf<Long>()

    // 已下线完成的玩家 ID
    private val flushedPlayerIds = linkedSetOf<Long>()

    // 期望停止的 World ID
    private val expectedWorldIds = linkedSetOf<Long>()

    // 已停止完成的 World ID
    private val flushedWorldIds = linkedSetOf<Long>()

    // 错误收集列表
    private val errors = mutableListOf<String>()

    override fun preStart() {
        super.preStart()

        //Actor 启动时打印日志。
        logger.info("{} started", self)
    }

    override fun postStop() {
        super.postStop()

        //Actor 停止时打印日志
        logger.info("{} stopped", self)
    }

    override fun createReceive(): Receive {
        return receiveBuilder()
            //启动停机流程
            .match(ShutdownStartReq::class.java) { handleStart(it) }
            //查询当前停机状态
            .match(ShutdownStatusReq::class.java) { sender.tell(status(), self) }
            //Gate 节点排水完成
            .match(GateDrainAck::class.java) { handleGateDrainAck(it) }
            //玩家下线完成
            .match(PlayerShutdownAck::class.java) { handlePlayerShutdownAck(it) }
            //World 停止完成
            .match(WorldShutdownAck::class.java) { handleWorldShutdownAck(it) }
            //阶段超时
            .match(ShutdownPhaseTimeout::class.java) { handlePhaseTimeout(it) }
            //移交协调器（自杀）
            .match(HandoffShutdownCoordinator::class.java) { context.stop(self) }
            .build()
    }

    private fun handleStart(command: ShutdownStartReq) {
        //只有处于 空闲、已完成、失败 这三种终态阶段时才能启动新的停机计划，否则直接返回当前状态
        if (
            phase !in setOf(
                ShutdownPhase.SHUTDOWN_PHASE_IDLE,
                ShutdownPhase.SHUTDOWN_PHASE_COMPLETED,
                ShutdownPhase.SHUTDOWN_PHASE_FAILED,
            )
        ) {
            sender.tell(status(), self)
            return
        }

        //重置状态并进入第一阶段：Gate 排水。
        reset(command)
        enterPhase(ShutdownPhase.SHUTDOWN_PHASE_DRAINING_GATES)

        //统计集群中当前活跃（Up / WeaklyUp）的 Gate 角色节点数量。
        expectedGateCount = activeRoleMemberCount(GameRoles.Gate)
        logger.info(
            "shutdown plan started planId={} requestedBy={} expectedGateCount={}",
            command.planId,
            command.requestedBy,
            expectedGateCount,
        )

        //通过分布式 PubSub 向 GATE_DRAIN_TOPIC 主题广播排水命令，告诉所有 Gate 节点开始拒绝新连接、通知已连接的玩家下线
        mediator.tell(
            Publish(
                GATE_DRAIN_TOPIC,
                GateDrainCommand.newBuilder()
                    .setPlanId(command.planId)
                    .setCoordinatorActor(self.encodeActorRef(node.system))
                    .build(),
            ),
            self,
        )

        //如果根本没有 Gate 节点，直接跳到 World 关闭阶段。
        if (expectedGateCount == 0) {
            beginWorldShutdown()
        }

        //返回当前停机状态给请求方。
        sender.tell(status(), self)
    }

    /*
    Gate 排水确认
     */
    private fun handleGateDrainAck(ack: GateDrainAck) {
        //校验：planId 必须匹配当前计划，且必须处于 Gate 排水阶段（忽略旧消息）。
        if (ack.planId != planId || phase != ShutdownPhase.SHUTDOWN_PHASE_DRAINING_GATES) {
            return
        }

        //记录该 Gate 已排水完成，并将该 Gate 上所有玩家 ID 加入期望下线列表。
        drainedGateNodes += ack.gateNodeId
        expectedPlayerIds += ack.playerIdList
        logger.info(
            "gate drained planId={} gateNodeId={} players={} drainedGateProgress={}",
            ack.planId,
            ack.gateNodeId,
            ack.playerIdCount,
            "${drainedGateNodes.size}/$expectedGateCount",
        )

        //当所有 Gate 都排水完成后，进入 Player 排水阶段。如果此时玩家已全部下线完（极少数情况），直接跳到 World 关闭。
        if (drainedGateNodes.size >= expectedGateCount) {
            enterPhase(ShutdownPhase.SHUTDOWN_PHASE_DRAINING_PLAYERS)
            if (expectedPlayerIds.all { it in flushedPlayerIds }) {
                beginWorldShutdown()
            }
        }
    }

    /*
    planId 校验 + 阶段校验（Gate 排水阶段和 Player 排水阶段都可以收到玩家下线确认）
     */
    private fun handlePlayerShutdownAck(ack: PlayerShutdownAck) {
        if (
            ack.shutdownPlanId != planId ||
            phase !in setOf(
                ShutdownPhase.SHUTDOWN_PHASE_DRAINING_GATES,
                ShutdownPhase.SHUTDOWN_PHASE_DRAINING_PLAYERS,
            )
        ) {
            return
        }

        //成功则记录已下线，失败则收集错误并立即进入失败阶段。
        if (ack.success) {
            flushedPlayerIds += ack.playerId
        } else {
            errors += "player ${ack.playerId} shutdown failed: ${ack.error}"
            enterPhase(ShutdownPhase.SHUTDOWN_PHASE_FAILED)
            return
        }
        logger.info(
            "player shutdown ack planId={} playerId={} flushedPlayerCount={}/{}",
            ack.shutdownPlanId,
            ack.playerId,
            flushedPlayerIds.size,
            expectedPlayerIds.size,
        )

        //当处于 Player 排水阶段且所有玩家都已下线，开始 World 停止
        if (
            phase == ShutdownPhase.SHUTDOWN_PHASE_DRAINING_PLAYERS &&
            expectedPlayerIds.all { it in flushedPlayerIds }
        ) {
            beginWorldShutdown()
        }
    }

    /*
    开始 World 关闭
     */
    private fun beginWorldShutdown() {
        //进入 World 停止阶段，获取所有 World ID。
        enterPhase(ShutdownPhase.SHUTDOWN_PHASE_STOPPING_WORLDS)
        expectedWorldIds.clear()
        expectedWorldIds += node.gameWorldIds
        logger.info("world shutdown started planId={} expectedWorldCount={}", planId, expectedWorldIds.size)

        //没有 World 则直接完成
        if (expectedWorldIds.isEmpty()) {
            enterPhase(ShutdownPhase.SHUTDOWN_PHASE_COMPLETED)
            return
        }

        //通过 Pekko Cluster Sharding 向每个 World 发送关闭请求
        expectedWorldIds.forEach { worldId ->
            node.worldSharding.tell(
                WorldShutdownReq.newBuilder()
                    .setWorldId(worldId)
                    .setShutdownPlanId(requireNotNull(planId))
                    .setCoordinatorActor(self.encodeActorRef(node.system))
                    .build(),
                self,
            )
        }
    }

    /*
    World 关闭确认
     */
    private fun handleWorldShutdownAck(ack: WorldShutdownAck) {
        //检验 planId 和阶段
        if (ack.shutdownPlanId != planId || phase != ShutdownPhase.SHUTDOWN_PHASE_STOPPING_WORLDS) {
            return
        }

        //成功记录 / 失败进入错误阶段
        if (ack.success) {
            flushedWorldIds += ack.worldId
        } else {
            errors += "world ${ack.worldId} shutdown failed: ${ack.error}"
            enterPhase(ShutdownPhase.SHUTDOWN_PHASE_FAILED)
            return
        }
        logger.info(
            "world shutdown ack planId={} worldId={} flushedWorldCount={}/{}",
            ack.shutdownPlanId,
            ack.worldId,
            flushedWorldIds.size,
            expectedWorldIds.size,
        )

        //所有 World 都停止完成后，进入完成阶段
        if (expectedWorldIds.all { it in flushedWorldIds }) {
            enterPhase(ShutdownPhase.SHUTDOWN_PHASE_COMPLETED)
            logger.info("shutdown plan completed planId={}", planId)
        }
    }

    /*
    阶段超时处理
     */
    private fun handlePhaseTimeout(timeout: ShutdownPhaseTimeout) {
        if (!timeout.isCurrent()) {
            return
        }

        //根据不同阶段生成有意义的超时错误信息，包括缺失的 ID 列表
        val error = when (phase) {
            ShutdownPhase.SHUTDOWN_PHASE_DRAINING_GATES ->
                "gate drain timeout: drained ${drainedGateNodes.size}/$expectedGateCount gate nodes"

            ShutdownPhase.SHUTDOWN_PHASE_DRAINING_PLAYERS -> {
                val missing = expectedPlayerIds - flushedPlayerIds
                "player shutdown timeout: flushed ${flushedPlayerIds.size}/${expectedPlayerIds.size} " +
                        "missing=${missing.formatIds()}"
            }

            ShutdownPhase.SHUTDOWN_PHASE_STOPPING_WORLDS -> {
                val missing = expectedWorldIds - flushedWorldIds
                "world shutdown timeout: flushed ${flushedWorldIds.size}/${expectedWorldIds.size} " +
                        "missing=${missing.formatIds()}"
            }

            else -> "shutdown timeout at phase=$phase"
        }

        //记录错误并进入失败阶段
        errors += error
        logger.error("shutdown plan failed planId={} phase={} error={}", planId, phase, error)
        enterPhase(ShutdownPhase.SHUTDOWN_PHASE_FAILED)
    }

    /*
    三重校验（planId + phase + generation）确保超时消息与当前状态完全匹配，且当前不在终态
     */
    private fun ShutdownPhaseTimeout.isCurrent(): Boolean {
        return planId == this@ShutdownCoordinatorActor.planId &&
                phase == this@ShutdownCoordinatorActor.phase &&
                generation == this@ShutdownCoordinatorActor.generation &&
                !this@ShutdownCoordinatorActor.phase.isTerminal()
    }

    /*
    设置新阶段并调度该阶段的超时定时器
     */
    private fun enterPhase(nextPhase: ShutdownPhase) {
        phase = nextPhase
        schedulePhaseTimeout(nextPhase)
    }

    private fun schedulePhaseTimeout(nextPhase: ShutdownPhase) {
        // 终态不需要超时
        if (nextPhase.isTerminal()) {
            return
        }

        // 没有 planId 则跳过
        val currentPlanId = planId ?: return

        // 没有配置超时的阶段跳过
        val timeout = timeoutFor(nextPhase) ?: return

        //用 Pekko scheduler 单次调度超时消息，到期后发给 self 。
        context.system.scheduler().scheduleOnce(
            timeout,
            self,
            ShutdownPhaseTimeout(currentPlanId, generation, nextPhase),
            context.dispatcher,
            self,
        )
    }

    /*
    阶段到超时时间的映射。
     */
    private fun timeoutFor(currentPhase: ShutdownPhase): Duration? {
        return when (currentPhase) {
            ShutdownPhase.SHUTDOWN_PHASE_DRAINING_GATES -> gateDrainTimeout
            ShutdownPhase.SHUTDOWN_PHASE_DRAINING_PLAYERS -> playerDrainTimeout
            ShutdownPhase.SHUTDOWN_PHASE_STOPPING_WORLDS -> worldStopTimeout
            else -> null
        }
    }

    /*
    重置所有状态变量， generation 自增用于区分不同批次的停机计划。
     */
    private fun reset(command: ShutdownStartReq) {
        generation += 1
        planId = command.planId
        requestedBy = command.requestedBy
        expectedGateCount = 0
        drainedGateNodes.clear()
        expectedPlayerIds.clear()
        flushedPlayerIds.clear()
        expectedWorldIds.clear()
        flushedWorldIds.clear()
        errors.clear()
    }

    /*
    统计集群中指定角色、处于 Up/WeaklyUp 状态的成员数
     */
    private fun activeRoleMemberCount(role: String): Int {
        return Cluster.get(context.system).state().members.count { member ->
            member.hasRole(role) && member.status() in setOf(MemberStatus.up(), MemberStatus.weaklyUp())
        }
    }

    /*
    构建当前停机进度的完整状态快照（Protobuf 消息）。
     */
    private fun status(): ShutdownStatusResp {
        val builder = ShutdownStatusResp.newBuilder()
            .setPhase(phase)
            .setExpectedGateCount(expectedGateCount)
            .setDrainedGateCount(drainedGateNodes.size)
            .setExpectedPlayerCount(expectedPlayerIds.size)
            .setFlushedPlayerCount(flushedPlayerIds.size)
            .setExpectedWorldCount(expectedWorldIds.size)
            .setFlushedWorldCount(flushedWorldIds.size)
            .addAllErrors(errors)
        planId?.let(builder::setPlanId)
        requestedBy?.let(builder::setRequestedBy)
        return builder.build()
    }

    companion object {
        fun props(node: GlobalNode): Props = Props.create(ShutdownCoordinatorActor::class.java, node)
    }
}
