/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions;

import io.atomix.raft.RaftRoleChangeListener;
import io.atomix.raft.RaftServer.Role;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.system.monitoring.DiskSpaceUsageListener;
import io.zeebe.broker.system.monitoring.HealthMetrics;
import io.zeebe.engine.processing.streamprocessor.StreamProcessor;
import io.zeebe.logstreams.storage.atomix.AtomixLogStorage;
import io.zeebe.snapshots.raft.PersistedSnapshotStore;
import io.zeebe.util.health.CriticalComponentsHealthMonitor;
import io.zeebe.util.health.FailureListener;
import io.zeebe.util.health.HealthMonitorable;
import io.zeebe.util.health.HealthStatus;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public final class ZeebePartition extends Actor
    implements RaftRoleChangeListener, HealthMonitorable, FailureListener, DiskSpaceUsageListener {

  private static final Logger LOG = Loggers.SYSTEM_LOGGER;

  private ActorFuture<Void> transitionFuture;
  private Role raftRole;

  private final String actorName;
  private FailureListener failureListener;
  private final HealthMetrics healthMetrics;
  private final RaftPartitionHealth raftPartitionHealth;
  private final ZeebePartitionHealth zeebePartitionHealth;
  private long term;

  private final ZeebePartitionState state;
  private final PartitionTransition transition;

  public ZeebePartition(final ZeebePartitionState state, final PartitionTransition transition) {
    this.state = state;
    this.transition = transition;

    state.setActor(actor);
    state.setDiskSpaceAvailable(true);

    actorName = buildActorName(state.getNodeId(), "ZeebePartition-" + state.getPartitionId());
    state.setComponentHealthMonitor(new CriticalComponentsHealthMonitor(actor, LOG));
    raftPartitionHealth =
        new RaftPartitionHealth(state.getRaftPartition(), actor, this::onRaftFailed);
    zeebePartitionHealth = new ZeebePartitionHealth(state.getPartitionId());
    healthMetrics = new HealthMetrics(state.getPartitionId());
    healthMetrics.setUnhealthy();
  }

  /**
   * Called by atomix on role change.
   *
   * @param newRole the new role of the raft partition
   */
  @Override
  public void onNewRole(final Role newRole, final long newTerm) {
    actor.run(() -> onRoleChange(newRole, newTerm));
  }

  private void onRoleChange(final Role newRole, final long newTerm) {
    term = newTerm;
    switch (newRole) {
      case LEADER:
        if (raftRole != Role.LEADER) {
          leaderTransition(newTerm);
        }
        break;
      case INACTIVE:
        inactiveTransition();
        break;
      case PASSIVE:
      case PROMOTABLE:
      case CANDIDATE:
      case FOLLOWER:
      default:
        if (raftRole == null || raftRole == Role.LEADER) {
          followerTransition(newTerm);
        }
        break;
    }

    LOG.debug("Partition role transitioning from {} to {}", raftRole, newRole);
    raftRole = newRole;
  }

  private void leaderTransition(final long newTerm) {
    onTransitionTo(transition::toLeader)
        .onComplete(
            (success, error) -> {
              if (error == null) {
                final List<ActorFuture<Void>> listenerFutures =
                    state.getPartitionListeners().stream()
                        .map(
                            l ->
                                l.onBecomingLeader(
                                    state.getPartitionId(), newTerm, state.getLogStream()))
                        .collect(Collectors.toList());
                actor.runOnCompletion(
                    listenerFutures,
                    t -> {
                      // Compare with the current term in case a new role transition
                      // happened
                      if (t != null && term == newTerm) {
                        onInstallFailure();
                      }
                    });
                onRecoveredInternal();
              } else {
                LOG.error("Failed to install leader partition {}", state.getPartitionId(), error);
                onInstallFailure();
              }
            });
  }

  private void followerTransition(final long newTerm) {
    onTransitionTo(transition::toFollower)
        .onComplete(
            (success, error) -> {
              if (error == null) {
                final List<ActorFuture<Void>> listenerFutures =
                    state.getPartitionListeners().stream()
                        .map(l -> l.onBecomingFollower(state.getPartitionId(), newTerm))
                        .collect(Collectors.toList());
                actor.runOnCompletion(
                    listenerFutures,
                    t -> {
                      // Compare with the current term in case a new role transition
                      // happened
                      if (t != null && term == newTerm) {
                        onInstallFailure();
                      }
                    });
                onRecoveredInternal();
              } else {
                LOG.error("Failed to install follower partition {}", state.getPartitionId(), error);
                onInstallFailure();
              }
            });
  }

  private ActorFuture<Void> inactiveTransition() {
    return onTransitionTo(this::transitionToInactive);
  }

  private void transitionToInactive(final CompletableActorFuture<Void> transitionComplete) {
    zeebePartitionHealth.setHealthStatus(HealthStatus.UNHEALTHY);
    transition.toInactive(transitionComplete);
  }

  private CompletableFuture<Void> onRaftFailed() {
    final CompletableFuture<Void> inactiveTransitionFuture = new CompletableFuture<>();
    actor.run(
        () -> {
          final ActorFuture<Void> transitionComplete = inactiveTransition();
          transitionComplete.onComplete(
              (v, t) -> {
                if (t != null) {
                  inactiveTransitionFuture.completeExceptionally(t);
                  return;
                }
                inactiveTransitionFuture.complete(null);
              });
        });
    return inactiveTransitionFuture;
  }

  private ActorFuture<Void> onTransitionTo(
      final Consumer<CompletableActorFuture<Void>> roleTransition) {
    final CompletableActorFuture<Void> nextTransitionFuture = new CompletableActorFuture<>();
    if (transitionFuture != null && !transitionFuture.isDone()) {
      // wait until previous transition is complete
      transitionFuture.onComplete((value, error) -> roleTransition.accept(nextTransitionFuture));
    } else {
      roleTransition.accept(nextTransitionFuture);
    }
    transitionFuture = nextTransitionFuture;
    return transitionFuture;
  }

  @Override
  public String getName() {
    return actorName;
  }

  @Override
  public void onActorStarting() {
    state.setAtomixLogStorage(
        AtomixLogStorage.ofPartition(state.getZeebeIndexMapping(), state.getRaftPartition()));
    state.getRaftPartition().addRoleChangeListener(this);
    state.getComponentHealthMonitor().addFailureListener(this);
    onRoleChange(state.getRaftPartition().getRole(), state.getRaftPartition().term());
  }

  @Override
  protected void onActorStarted() {
    state.getComponentHealthMonitor().startMonitoring();
    state
        .getComponentHealthMonitor()
        .registerComponent(raftPartitionHealth.getName(), raftPartitionHealth);
    // Add a component that keep track of health of ZeebePartition. This way
    // criticalComponentsHealthMonitor can monitor the health of ZeebePartition similar to other
    // components.
    state
        .getComponentHealthMonitor()
        .registerComponent(zeebePartitionHealth.getName(), zeebePartitionHealth);
  }

  @Override
  protected void onActorClosing() {
    final var future = new CompletableActorFuture<Void>();
    transition.toInactive(future);
    future.onComplete(
        (nothing, err) -> {
          state.getRaftPartition().removeRoleChangeListener(this);

          state.getComponentHealthMonitor().removeComponent(raftPartitionHealth.getName());
          raftPartitionHealth.close();
        });
  }

  @Override
  protected void handleFailure(final Exception failure) {
    LOG.warn("Uncaught exception in {}.", actorName, failure);
    // Most probably exception happened in the middle of installing leader or follower services
    // because this actor is not doing anything else
    onInstallFailure();
  }

  @Override
  public void onFailure() {
    actor.run(
        () -> {
          healthMetrics.setUnhealthy();
          if (failureListener != null) {
            failureListener.onFailure();
          }
        });
  }

  @Override
  public void onRecovered() {
    actor.run(
        () -> {
          healthMetrics.setHealthy();
          if (failureListener != null) {
            failureListener.onRecovered();
          }
        });
  }

  private void onInstallFailure() {
    zeebePartitionHealth.setHealthStatus(HealthStatus.UNHEALTHY);
    if (state.getRaftPartition().getRole() == Role.LEADER) {
      LOG.info("Unexpected failures occurred when installing leader services, stepping down");
      state.getRaftPartition().stepDown();
    }
  }

  private void onRecoveredInternal() {
    zeebePartitionHealth.setHealthStatus(HealthStatus.HEALTHY);
  }

  @Override
  public HealthStatus getHealthStatus() {
    return state.getComponentHealthMonitor().getHealthStatus();
  }

  @Override
  public void addFailureListener(final FailureListener failureListener) {
    actor.run(() -> this.failureListener = failureListener);
  }

  @Override
  public void onDiskSpaceNotAvailable() {
    actor.call(
        () -> {
          state.setDiskSpaceAvailable(false);
          if (state.getStreamProcessor() != null) {
            LOG.warn("Disk space usage is above threshold. Pausing stream processor.");
            state.getStreamProcessor().pauseProcessing();
          }
        });
  }

  @Override
  public void onDiskSpaceAvailable() {
    actor.call(
        () -> {
          state.setDiskSpaceAvailable(true);
          if (state.getStreamProcessor() != null && !state.shouldPauseProcessing()) {
            LOG.info("Disk space usage is below threshold. Resuming stream processor.");
            state.getStreamProcessor().resumeProcessing();
          }
        });
  }

  public ActorFuture<Void> pauseProcessing() {
    final CompletableActorFuture<Void> completed = new CompletableActorFuture<>();
    actor.call(
        () -> {
          state.setProcessingPaused(true);
          if (state.getStreamProcessor() != null) {
            state.getStreamProcessor().pauseProcessing().onComplete(completed);
          } else {
            completed.complete(null);
          }
        });
    return completed;
  }

  public void resumeProcessing() {
    actor.call(
        () -> {
          state.setProcessingPaused(false);
          if (state.getStreamProcessor() != null && !state.shouldPauseProcessing()) {
            state.getStreamProcessor().resumeProcessing();
          }
        });
  }

  public int getPartitionId() {
    return state.getPartitionId();
  }

  public PersistedSnapshotStore getSnapshotStore() {
    return state.getRaftPartition().getServer().getPersistedSnapshotStore();
  }

  public void triggerSnapshot() {
    actor.call(
        () -> {
          if (state.getSnapshotDirector() != null) {
            state.getSnapshotDirector().forceSnapshot();
          }
        });
  }

  public ActorFuture<Optional<StreamProcessor>> getStreamProcessor() {
    return actor.call(() -> Optional.ofNullable(state.getStreamProcessor()));
  }
}
