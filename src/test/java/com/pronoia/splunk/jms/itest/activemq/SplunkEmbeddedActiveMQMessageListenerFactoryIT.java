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

package com.pronoia.splunk.jms.itest.activemq;

import static com.pronoia.junit.asserts.activemq.EmbeddedBrokerAssert.assertMessageCount;

import com.pronoia.junit.activemq.EmbeddedActiveMQBroker;
import com.pronoia.splunk.eventcollector.client.SimpleEventCollectorClient;
import com.pronoia.splunk.jms.activemq.SplunkEmbeddedActiveMQMessageListenerFactory;
import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the  class.
 */
public class SplunkEmbeddedActiveMQMessageListenerFactoryIT {
  static final String DESTINATION_NAME = "audit.in";

  public EmbeddedActiveMQBroker broker;

  Logger log = LoggerFactory.getLogger(this.getClass());

  SimpleEventCollectorClient httpecClient = new SimpleEventCollectorClient();

  SplunkEmbeddedActiveMQMessageListenerFactory instance;

  @Before
  public void setUp() throws Exception {
    broker = new EmbeddedActiveMQBroker();
    broker.getBrokerService().setUseJmx(true);
    broker.start();
    broker.sendTextMessage(DESTINATION_NAME, "TEST MESSAGE");

    String brokerURL = String.format("vm://%s?create=false&waitForStart=5000", broker.getBrokerName());

    instance = new SplunkEmbeddedActiveMQMessageListenerFactory();
    instance.setUserName("admin");
    instance.setPassword("admin");

    JmsMessageEventBuilder eventBuilder = new JmsMessageEventBuilder();

    eventBuilder.setIndex("fuse-dev");
    eventBuilder.setSource("test-source");
    eventBuilder.setSourcetype("test-sourcetype");

    instance.setSplunkEventBuilder(eventBuilder);

    instance.setDestinationName(DESTINATION_NAME);

    httpecClient = new SimpleEventCollectorClient();

    // Local Settings
    httpecClient.setHost("localhost");
    httpecClient.setPort(8088);
    httpecClient.setAuthorizationToken("5DA702AD-D855-4679-9CDE-A398494BE854");
    httpecClient.disableCertificateValidation();

    // UCLA Settings
    // httpecClient.setHost("lstsplkap19");
    // httpecClient.setPort(8088);
    // httpecClient.setAuthorizationToken("902ADE3D-2895-47F0-ABE6-4981DB2ABE9C");
    // httpecClient.disableCertificateValidation();

    instance.setSplunkClient(httpecClient);
  }

  @After
  public void tearDown() throws Exception {
    log.info("Stopping message listener");
    instance.stop();
  }

  /**
   * Description of test.
   *
   * @throws Exception in the event of a test error.
   */
  @Test
  public void testOnMessage() throws Exception {
    Thread.sleep(500);
    instance.start();

    Thread.sleep(30000);
    assertMessageCount(broker, DESTINATION_NAME, 0);
  }

}