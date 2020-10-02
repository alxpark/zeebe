/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl.components;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.system.partitions.Component;
import io.zeebe.broker.system.partitions.ZeebePartitionState;
import io.zeebe.db.ZeebeDb;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class ZeebeDbComponent implements Component<ZeebeDb> {

  @Override
  public ActorFuture<ZeebeDb> open(final ZeebePartitionState state) {
    state
        .getSnapshotStoreSupplier()
        .getPersistedSnapshotStore(state.getRaftPartition().name())
        .addSnapshotListener(state.getSnapshotController());

    final ZeebeDb zeebeDb;
    try {
      state.getSnapshotController().recover();
      zeebeDb = state.getSnapshotController().openDb();
    } catch (final Exception e) {
      Loggers.SYSTEM_LOGGER.error("Failed to recover from snapshot", e);

      return CompletableActorFuture.completedExceptionally(
          new IllegalStateException(
              String.format(
                  "Unexpected error occurred while recovering snapshot controller during leader partition install for partition %d",
                  state.getPartitionId()),
              e));
    }

    return CompletableActorFuture.completed(zeebeDb);
  }

  @Override
  public ActorFuture<Void> close(final ZeebePartitionState state) {
    state.setZeebeDb(null);
    return CompletableActorFuture.completed(null);
  }

  @Override
  public void onOpen(final ZeebePartitionState state, final ZeebeDb zeebeDb) {
    state.setZeebeDb(zeebeDb);
  }

  @Override
  public String getName() {
    return "ZeebeDb";
  }
}
