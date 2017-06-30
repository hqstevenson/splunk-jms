/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pronoia.splunk.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import javax.jms.Destination;

import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Configuration Tests for the SplunkJmsMessageListener class.
 */
public class SplunkJmsMessageListenerConfigurationTest {
  SplunkJmsMessageListener instance;

  @Before
  public void setUp() throws Exception {
    instance = new SplunkJmsMessageListener();
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testGetDestinationName() throws Exception {
    assertNull(instance.destinationName);

    final String expected = "myDestinationName";
    instance.destinationName = expected;
    assertEquals(expected, instance.getDestinationName());
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testSetDestinationName() throws Exception {
    assertNull(instance.destinationName);

    final String expected = "myDestinationName";
    instance.setDestinationName(expected);
    assertEquals(expected, instance.destinationName.toString());
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Ignore("SplunkJmsMessageListenerConfigurationTest.testVerifyConfiguration method not yet implemented")
  @Test
  public void testVerifyConfiguration() throws Exception {
    throw new UnsupportedOperationException("SplunkJmsMessageListenerConfigurationTest.testVerifyConfiguration not yet implemented");
  }

}