/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl.components;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.state.StatePositionSupplier;
import io.zeebe.broker.system.partitions.Component;
import io.zeebe.broker.system.partitions.ZeebePartitionState;
import io.zeebe.broker.system.partitions.impl.AtomixRecordEntrySupplierImpl;
import io.zeebe.broker.system.partitions.impl.StateControllerImpl;
import io.zeebe.engine.state.DefaultZeebeDbFactory;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class StateControllerComponent implements Component<StateControllerImpl> {

  @Override
  public ActorFuture<StateControllerImpl> open(final ZeebePartitionState state) {
    final var runtimeDirectory =
        state.getRaftPartition().dataDirectory().toPath().resolve("runtime");
    final var databaseCfg = state.getBrokerCfg().getData().getRocksdb();

    final var stateController =
        new StateControllerImpl(
            state.getPartitionId(),
            DefaultZeebeDbFactory.defaultFactory(databaseCfg.getColumnFamilyOptions()),
            state
                .getSnapshotStoreSupplier()
                .getConstructableSnapshotStore(state.getRaftPartition().name()),
            state
                .getSnapshotStoreSupplier()
                .getReceivableSnapshotStore(state.getRaftPartition().name()),
            runtimeDirectory,
            state.getSnapshotReplication(),
            new AtomixRecordEntrySupplierImpl(
                state.getZeebeIndexMapping(), state.getRaftLogReader()),
            StatePositionSupplier::getHighestExportedPosition);
    // TODO(miguel): check warning

    return CompletableActorFuture.completed(stateController);
  }

  @Override
  public ActorFuture<Void> close(final ZeebePartitionState state) {
    try {
      if (state.getSnapshotController() != null) {
        state.getSnapshotController().close();
      }
    } catch (final Exception e) {
      Loggers.SYSTEM_LOGGER.error(
          "Unexpected error closing state replication for partition {}", state.getPartitionId(), e);
    } finally {
      state.setSnapshotController(null);
    }

    return CompletableActorFuture.completed(null);
  }

  @Override
  public void onOpen(final ZeebePartitionState state, final StateControllerImpl stateController) {
    state.setSnapshotController(stateController);
  }

  @Override
  public String getName() {
    return "StateController";
  }
}
