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

import static org.junit.Assert.assertNotNull;

import com.pronoia.junit.activemq.EmbeddedActiveMQBroker;
import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;
import com.pronoia.stub.httpec.EventCollectorClientStub;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the  class.
 */
public class SplunkJmsMessageConsumerTest {
  static final String DESTINATION_NAME = "audit.in";

  @Rule
  public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

  Logger log = LoggerFactory.getLogger(this.getClass());

  EventCollectorClientStub clientStub = new EventCollectorClientStub();
  SplunkJmsMessageConsumer instance;

  @Before
  public void setUp() throws Exception {
    String brokerURL = String.format("vm://%s?create=false&waitForStart=5000", broker.getBrokerName());

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
    connectionFactory.setBrokerURL(brokerURL);
    connectionFactory.setUserName("admin");
    connectionFactory.setPassword("admin");

    instance = new SplunkJmsMessageConsumer(DESTINATION_NAME);
    instance.setConnectionFactory(connectionFactory);
    instance.setSplunkEventBuilder(new JmsMessageEventBuilder());

    log.info("Starting instance");
    instance.setSplunkClient(clientStub);

    instance.start();
  }

  @After
  public void tearDown() throws Exception {
    log.info("Stopping instance");
    instance.stop();
  }

  /**
   * Description of test.
   *
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testSingleMessage() throws Exception {
    broker.sendTextMessage("queue://audit.in", "Dummy Message Body");

    Thread.sleep(instance.getInitialDelaySeconds() * 1000 + instance.getReceiveTimeoutMillis() * 2);  // Wait for the listener to consume the message

    assertNotNull(clientStub.lastEvent);
  }

}