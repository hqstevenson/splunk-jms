/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pronoia.splunk.jms.itest;

import com.pronoia.junit.activemq.EmbeddedActiveMQBroker;
import com.pronoia.junit.asserts.activemq.EmbeddedBrokerAssert;

import com.pronoia.splunk.eventcollector.client.SimpleEventCollectorClient;
import com.pronoia.splunk.jms.SplunkJmsMessageConsumer;
import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;

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
public class SplunkJmsMessageConsumerIT {
    static final String DESTINATION_NAME = "audit.in";

    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker();

    Logger log = LoggerFactory.getLogger(this.getClass());

    SimpleEventCollectorClient httpecClient = new SimpleEventCollectorClient();

    SplunkJmsMessageConsumer instance;

    @Before
    public void setUp() throws Exception {
        String brokerURL = String.format("vm://%s?create=false&waitForStart=5000", broker.getBrokerName());

        instance = new SplunkJmsMessageConsumer(DESTINATION_NAME);
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();

        connectionFactory.setBrokerURL(brokerURL);
        connectionFactory.setUserName("admin");
        connectionFactory.setPassword("admin");
        instance.setConnectionFactory(connectionFactory);

        JmsMessageEventBuilder builder = new JmsMessageEventBuilder();

        builder.setIndex("fuse-dev");
        builder.setSource("test-source");
        builder.setSourcetype("test-sourcetype");

        instance.setSplunkEventBuilder(builder);

        httpecClient = new SimpleEventCollectorClient();

        // Local Settings
        httpecClient.setHost("localhost");
        httpecClient.setPort(8088);
        httpecClient.setAuthorizationToken("5DA702AD-D855-4679-9CDE-A398494BE854");
        httpecClient.disableCertificateValidation();

        instance.setSplunkClient(httpecClient);

        instance.start();
    }

    @After
    public void tearDown() throws Exception {
        instance.stop();
    }

    /**
     * Description of test.
     *
     * @throws Exception in the event of a test error.
     */
    @Test
    public void testSendMessage() throws Exception {
        broker.sendTextMessage(DESTINATION_NAME, "TEST MESSAGE FROM JMS CONSUMER");

        // Give the instance a little time to process the message
        Thread.sleep(instance.getReceiveTimeoutMillis() * 3);

        EmbeddedBrokerAssert.assertMessageCount(broker, DESTINATION_NAME, 0);

        instance.stop();
        // Give the instance a little time to shutdown gracefully
        Thread.sleep(instance.getReceiveTimeoutMillis() * 2);

        log.info("Test ended");
    }

}