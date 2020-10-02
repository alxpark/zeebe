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
import io.zeebe.broker.system.partitions.SnapshotReplication;
import io.zeebe.broker.system.partitions.ZeebePartitionState;
import io.zeebe.broker.system.partitions.impl.NoneSnapshotReplication;
import io.zeebe.broker.system.partitions.impl.StateReplication;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class SnapshotReplicationComponent implements Component<SnapshotReplication> {

  @Override
  public ActorFuture<SnapshotReplication> open(final ZeebePartitionState state) {
    final SnapshotReplication replication =
        shouldReplicateSnapshots(state)
            ? new StateReplication(
                state.getMessagingService(), state.getPartitionId(), state.getNodeId())
            : new NoneSnapshotReplication();
    return CompletableActorFuture.completed(replication);
  }

  @Override
  public ActorFuture<Void> close(final ZeebePartitionState state) {
    try {
      if (state.getSnapshotReplication() != null) {
        state.getSnapshotReplication().close();
      }
    } catch (final Exception e) {
      Loggers.SYSTEM_LOGGER.error(
          "Unexpected error closing state replication for partition {}", state.getPartitionId(), e);
    } finally {
      state.setSnapshotReplication(null);
    }

    return CompletableActorFuture.completed(null);
  }

  @Override
  public void onOpen(
      final ZeebePartitionState state, final SnapshotReplication snapshotReplication) {
    state.setSnapshotReplication(snapshotReplication);
  }

  @Override
  public String getName() {
    return "SnapshotReplication";
  }

  private boolean shouldReplicateSnapshots(final ZeebePartitionState state) {
    return state.getBrokerCfg().getCluster().getReplicationFactor() > 1;
  }
}
