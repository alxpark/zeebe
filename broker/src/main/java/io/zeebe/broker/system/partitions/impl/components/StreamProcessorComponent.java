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
import io.zeebe.engine.processing.streamprocessor.StreamProcessor;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

public class StreamProcessorComponent implements Component<StreamProcessor> {

  @Override
  public ActorFuture<StreamProcessor> open(final ZeebePartitionState state) {
    final StreamProcessor streamProcessor = createStreamProcessor(state);

    final ActorFuture<Void> streamProcFuture = streamProcessor.openAsync();
    final CompletableActorFuture<StreamProcessor> future = new CompletableActorFuture<>();

    streamProcFuture.onComplete(
        (nothing, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
          } else {
            future.complete(streamProcessor);
          }
        });

    return future;
  }

  @Override
  public ActorFuture<Void> close(final ZeebePartitionState state) {
    state.getComponentHealthMonitor().removeComponent(state.getStreamProcessor().getName());
    return state.getStreamProcessor().closeAsync();
  }

  @Override
  public void onOpen(final ZeebePartitionState state, final StreamProcessor streamProcessor) {
    state.setStreamProcessor(streamProcessor);

    if (shouldPauseStreamProcessing(state)) {
      streamProcessor.pauseProcessing();
    }

    state.getComponentHealthMonitor().registerComponent(streamProcessor.getName(), streamProcessor);
  }

  @Override
  public String getName() {
    return "StreamProcessor";
  }

  private StreamProcessor createStreamProcessor(final ZeebePartitionState state) {
    return StreamProcessor.builder()
        .logStream(state.getLogStream())
        .actorScheduler(state.getScheduler())
        .zeebeDb(state.getZeebeDb())
        .nodeId(state.getNodeId())
        .commandResponseWriter(state.getCommandApiService().newCommandResponseWriter())
        .onProcessedListener(
            state.getCommandApiService().getOnProcessedListener(state.getPartitionId()))
        .streamProcessorFactory(
            processingContext -> {
              final ActorControl actor = processingContext.getActor();
              final ZeebeState zeebeState = processingContext.getZeebeState();
              return state
                  .getTypedRecordProcessorsFactory()
                  .createTypedStreamProcessor(actor, zeebeState, processingContext);
            })
        .build();
  }

  public boolean shouldPauseStreamProcessing(final ZeebePartitionState state) {
    return !state.isDiskSpaceAvailable() || state.isProcessingPaused();
  }
}
