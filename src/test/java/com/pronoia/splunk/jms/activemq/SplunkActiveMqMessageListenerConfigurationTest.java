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

package com.pronoia.splunk.jms.activemq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.pronoia.splunk.jms.SplunkJmsMessageListener;

import javax.jms.Destination;

import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Configuration Tests for the SplunkJmsMessageListener class.
 */
public class SplunkActiveMqMessageListenerConfigurationTest {
  SplunkActiveMQMessageListener instance;

  @Before
  public void setUp() throws Exception {
    instance = new SplunkActiveMQMessageListener();
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testGetBrokerURL() throws Exception {
    assertNull(instance.brokerURL);

    final String expected = "vm://dummy-broker?create=false";
    instance.brokerURL = expected;
    assertEquals(expected, instance.getBrokerURL());
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testSetBrokerURL() throws Exception {
    assertNull(instance.brokerURL);

    final String expected = "vm://dummy-broker?create=false";
    instance.setBrokerURL(expected);
    assertEquals(expected, instance.brokerURL);
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testGetUserName() throws Exception {
    assertNull(instance.userName);

    final String expected = "myUserName";
    instance.userName = expected;
    assertEquals(expected, instance.getUserName());
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testSetUserName() throws Exception {
    assertNull(instance.userName);

    final String expected = "myUserName";
    instance.setUserName(expected);
    assertEquals(expected, instance.userName);
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testGetPassword() throws Exception {
    assertNull(instance.password);

    final String expected = "myPassword";
    instance.password = expected;
    assertEquals(expected, instance.getPassword());
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testSetPassword() throws Exception {
    assertNull(instance.password);

    final String expected = "myPassword";
    instance.setPassword(expected);
    assertEquals(expected, instance.password);
  }

  /**
   * @throws Exception in the event of a test error.
   */
  @Ignore("SplunkActiveMqMessageListenerConfigurationTest.testVerifyConfiguration method not yet implemented")
  @Test
  public void testVerifyConfiguration() throws Exception {
    throw new UnsupportedOperationException("SplunkActiveMqMessageListenerConfigurationTest.testVerifyConfiguration not yet implemented");
  }

}