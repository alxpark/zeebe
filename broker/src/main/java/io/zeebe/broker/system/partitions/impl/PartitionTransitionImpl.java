/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.system.partitions.Component;
import io.zeebe.broker.system.partitions.PartitionTransition;
import io.zeebe.broker.system.partitions.ZeebePartitionState;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;

public class PartitionTransitionImpl<T> implements PartitionTransition {

  private static final Logger LOG = Loggers.SYSTEM_LOGGER;

  private final ZeebePartitionState state;
  private final List<Component<T>> leaderComponents;
  private final List<Component<T>> followerComponents;
  private final List<Component<T>> openedComponents = new ArrayList<>();

  public PartitionTransitionImpl(
      final ZeebePartitionState state,
      final List<Component<T>> leaderComponents,
      final List<Component<T>> followerComponents) {
    this.state = state;
    this.leaderComponents = leaderComponents;
    this.followerComponents = followerComponents;
  }

  @Override
  public void toFollower(final CompletableActorFuture<Void> future) {
    closePartition()
        .onComplete(
            (nothing, err) -> {
              if (err == null) {
                installComponents(future, new ArrayList<>(followerComponents));
              } else {
                future.completeExceptionally(err);
              }
            });
  }

  @Override
  public void toLeader(final CompletableActorFuture<Void> future) {
    closePartition()
        .onComplete(
            (nothing, err) -> {
              // TODO(miguel): try to stay as follower?
              if (err == null) {
                installComponents(future, new ArrayList<>(leaderComponents));
              } else {
                future.completeExceptionally(err);
              }
            });
  }

  @Override
  public void toInactive(final CompletableActorFuture<Void> future) {
    closePartition()
        .onComplete(
            (nothing, err) -> {
              if (err == null) {
                future.complete(null);
              } else {
                future.completeExceptionally(err);
              }
            });
  }

  private void installComponents(
      final CompletableActorFuture<Void> future, final List<Component<T>> components) {
    if (components.isEmpty()) {
      future.complete(null);
      return;
    }

    final Component<T> component = components.remove(0);
    component
        .open(state)
        .onComplete(
            (value, err) -> {
              if (err != null) {
                LOG.debug(
                    "Expected to open component '{}' but failed with", component.getName(), err);
              } else {
                component.onOpen(state, value);
                openedComponents.add(component);
                installComponents(future, components);
              }
            });
  }

  private CompletableActorFuture<Void> closePartition() {
    // caution: this method may be called concurrently on role transition due to closing the actor
    // - first, it is called by one of the transitionTo...() methods
    // - then it is called by onActorClosing()
    // TODO(miguel): this breaks the abstraction - why is it necessary?
    //    state.setStreamProcessor(null);
    //    state.setSnapshotDirector(null);

    final var closingStepsInReverseOrder = new ArrayList<>(openedComponents);
    Collections.reverse(closingStepsInReverseOrder);

    final var closingPartitionFuture = new CompletableActorFuture<Void>();
    stepByStepClosing(closingPartitionFuture, closingStepsInReverseOrder);

    return closingPartitionFuture;
  }

  private void stepByStepClosing(
      final CompletableActorFuture<Void> closingFuture, final List<Component<T>> actorsToClose) {
    if (actorsToClose.isEmpty()) {
      closingFuture.complete(null);
      return;
    }

    final Component<?> component = actorsToClose.remove(0);
    LOG.debug("Closing Zeebe-Partition-{}: {}", state.getPartitionId(), component.getName());

    final ActorFuture<Void> closeFuture = component.close(state);
    closeFuture.onComplete(
        (v, t) -> {
          if (t == null) {
            LOG.debug(
                "Closing Zeebe-Partition-{}: {} closed successfully",
                state.getPartitionId(),
                component.getName());

            // remove the completed step from the list in case that the closing is interrupted
            openedComponents.remove(component);

            // closing the remaining steps
            stepByStepClosing(closingFuture, actorsToClose);

          } else {
            // TODO(miguel): complete with exception but try to close the rest of the components
            LOG.error(
                "Closing Zeebe-Partition-{}: {} failed to close",
                state.getPartitionId(),
                component.getName(),
                t);
            closingFuture.completeExceptionally(t);
          }
        });
  }
}
