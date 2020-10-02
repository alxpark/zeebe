/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.partitions;

import io.zeebe.util.sched.future.CompletableActorFuture;

public interface PartitionTransition {
  void toFollower(CompletableActorFuture<Void> future);

  void toLeader(CompletableActorFuture<Void> future);

  void toInactive(CompletableActorFuture<Void> future);
}
