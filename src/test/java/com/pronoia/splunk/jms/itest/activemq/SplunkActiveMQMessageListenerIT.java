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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.pronoia.splunk.jms.activemq.SplunkActiveMQMessageListener;
import com.pronoia.stub.httpec.EventCollectorClientStub;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the  class.
 */
public class SplunkActiveMQMessageListenerIT {
  static final String DESTINATION_NAME = "queue://audit.in";

  Logger log = LoggerFactory.getLogger(this.getClass());

  EventCollectorClientStub clientStub = new EventCollectorClientStub();

  SplunkActiveMQMessageListener instance;

  @Before
  public void setUp() throws Exception {
    instance = new SplunkActiveMQMessageListener();
    instance.setBrokerURL("tcp://0.0.0.0:3106");
    instance.setUserName("admin");
    instance.setPassword("admin");

    instance.setDestinationName(DESTINATION_NAME);

    log.info("Starting message listener");
    instance.setSplunkClient(clientStub);

    instance.start();
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
    Thread.sleep(5000);

    // assertNotNull(clientStub.lastEvent);

    log.info("Stop external broker now");
    Thread.sleep(60000);
  }

}