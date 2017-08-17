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

package com.pronoia.splunk.jms.activemq.internal;

import com.pronoia.splunk.jms.SplunkJmsMessageListener;
import com.pronoia.splunk.jms.activemq.SplunkEmbeddedActiveMQMessageListenerFactory;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageListenerStartupTask implements Runnable {
  Logger log = LoggerFactory.getLogger(this.getClass());

  SplunkEmbeddedActiveMQMessageListenerFactory listenerFactory;
  String canonicalNameString;

  public MessageListenerStartupTask(SplunkEmbeddedActiveMQMessageListenerFactory listenerFactory, String canonicalNameString) {
    this.listenerFactory = listenerFactory;
    this.canonicalNameString = canonicalNameString;
  }

  @Override
  public void run() {
    log.info("Preparing to start {}", canonicalNameString);

    SplunkJmsMessageListener messageListener = listenerFactory.getMessageListener(canonicalNameString);
    if (messageListener != null && !messageListener.isRunning()) {
      messageListener.start();
    }
  }

}