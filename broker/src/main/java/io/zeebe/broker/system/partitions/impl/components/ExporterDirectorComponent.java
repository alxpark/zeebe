/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions.impl.components;

import io.zeebe.broker.exporter.stream.ExporterDirector;
import io.zeebe.broker.exporter.stream.ExporterDirectorContext;
import io.zeebe.broker.system.partitions.Component;
import io.zeebe.broker.system.partitions.ZeebePartitionState;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class ExporterDirectorComponent implements Component<ExporterDirector> {
  private static final int EXPORTER_PROCESSOR_ID = 1003;
  private static final String EXPORTER_NAME = "Exporter-%d";

  @Override
  public ActorFuture<ExporterDirector> open(final ZeebePartitionState state) {
    final var exporterDescriptors = state.getExporterRepository().getExporters().values();

    final ExporterDirectorContext context =
        new ExporterDirectorContext()
            .id(EXPORTER_PROCESSOR_ID)
            .name(
                Actor.buildActorName(
                    state.getNodeId(), String.format(EXPORTER_NAME, state.getPartitionId())))
            .logStream(state.getLogStream())
            .zeebeDb(state.getZeebeDb())
            .descriptors(exporterDescriptors);

    return CompletableActorFuture.completed(new ExporterDirector(context));
  }

  @Override
  public ActorFuture<Void> close(final ZeebePartitionState state) {
    return state.getExporterDirector().closeAsync();
  }

  @Override
  public void onOpen(final ZeebePartitionState state, final ExporterDirector exporterDirector) {
    state.setExporterDirector(exporterDirector);
    exporterDirector.startAsync(state.getScheduler());
  }

  @Override
  public String getName() {
    return null;
  }
}
