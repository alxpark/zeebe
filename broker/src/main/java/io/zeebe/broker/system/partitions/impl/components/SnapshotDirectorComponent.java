/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl.components;

import io.zeebe.broker.system.partitions.Component;
import io.zeebe.broker.system.partitions.ZeebePartitionState;
import io.zeebe.broker.system.partitions.impl.AsyncSnapshotDirector;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import java.time.Duration;

public class SnapshotDirectorComponent implements Component<AsyncSnapshotDirector> {

  @Override
  public ActorFuture<AsyncSnapshotDirector> open(final ZeebePartitionState state) {
    final Duration snapshotPeriod = state.getBrokerCfg().getData().getSnapshotPeriod();
    final AsyncSnapshotDirector snapshotDirector =
        new AsyncSnapshotDirector(
            state.getNodeId(),
            state.getStreamProcessor(),
            state.getSnapshotController(),
            state.getLogStream(),
            snapshotPeriod);

    return CompletableActorFuture.completed(snapshotDirector);
  }

  @Override
  public ActorFuture<Void> close(final ZeebePartitionState state) {
    return state.getSnapshotDirector().closeAsync();
  }

  @Override
  public void onOpen(
      final ZeebePartitionState state, final AsyncSnapshotDirector asyncSnapshotDirector) {
    state.setSnapshotDirector(asyncSnapshotDirector);
    state.getScheduler().submitActor(asyncSnapshotDirector);
  }

  @Override
  public String getName() {
    return "AsyncSnapshotDirector";
  }
}
