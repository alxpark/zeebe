/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test;

import org.junit.Assert;
import org.junit.Test;

public class FailingTest {

  @Test
  public void shouldFail() {
    // This is just a small change, to cause another build based on a real commit
    Assert.fail("This is a failure made specifically for testing the CI build");
  }
}
