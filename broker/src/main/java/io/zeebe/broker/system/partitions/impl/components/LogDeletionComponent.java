/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl.components;

import io.zeebe.broker.logstreams.AtomixLogCompactor;
import io.zeebe.broker.logstreams.LogCompactor;
import io.zeebe.broker.logstreams.LogDeletionService;
import io.zeebe.broker.system.partitions.Component;
import io.zeebe.broker.system.partitions.ZeebePartitionState;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class LogDeletionComponent implements Component<LogDeletionService> {

  @Override
  public ActorFuture<LogDeletionService> open(final ZeebePartitionState state) {
    final LogCompactor logCompactor = new AtomixLogCompactor(state.getRaftPartition().getServer());
    final LogDeletionService deletionService =
        new LogDeletionService(
            state.getNodeId(),
            state.getPartitionId(),
            logCompactor,
            state
                .getSnapshotStoreSupplier()
                .getPersistedSnapshotStore(state.getRaftPartition().name()));

    return CompletableActorFuture.completed(deletionService);
  }

  @Override
  public ActorFuture<Void> close(final ZeebePartitionState state) {
    return state.getLogDeletionService().closeAsync();
  }

  @Override
  public void onOpen(final ZeebePartitionState state, final LogDeletionService deletionService) {
    state.setLogDeletionService(deletionService);
    state.getScheduler().submitActor(deletionService);
  }

  @Override
  public String getName() {
    return "LogDeletionService";
  }
}
