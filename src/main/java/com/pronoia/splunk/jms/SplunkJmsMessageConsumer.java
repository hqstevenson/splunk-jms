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

import com.pronoia.splunk.jms.internal.NamedThreadFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.Message;

/**
 * Receives JMS Messages from an ActiveMQ broker in the same JVM and delivers them to Splunk using the HTTP Event
 * Collector.
 *
 * This class is intended to be run on a schedule with a fixed delay.
 */
public class SplunkJmsMessageConsumer extends SplunkJmsConsumerSupport implements Runnable {
  long receiveTimeoutMillis = 1000;
  long initialDelaySeconds = 1;
  long delaySeconds = 60;
  /**
   * Start the JMS Consumer and process messages.
   *
   * TODO:  Fixup error handling
   */
  ScheduledExecutorService scheduledExecutorService;
  boolean scheduled = false;

  public SplunkJmsMessageConsumer() {
  }

  public SplunkJmsMessageConsumer(String destinationName) {
    super(destinationName, false);
  }

  public SplunkJmsMessageConsumer(String destinationName, boolean useTopic) {
    super(destinationName, useTopic);
  }

  public void start() {
    log.info("Starting JMS consumer for {}", destinationName);
    verifyConfiguration();

    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(this.getClass().getSimpleName() + "{" + destinationName + "}"));

    scheduledExecutorService.scheduleWithFixedDelay(this, initialDelaySeconds, delaySeconds, TimeUnit.SECONDS);
    scheduled = true;
  }

  public void stop() {
    scheduled = false;
    if (scheduledExecutorService != null) {
      log.info("Shutting-down executor service");
      scheduledExecutorService.shutdownNow();
      log.info("Executor service shutdown");
      scheduledExecutorService = null;
    }
  }

  public long getReceiveTimeoutMillis() {
    return receiveTimeoutMillis;
  }

  public void setReceiveTimeoutMillis(long receiveTimeoutMillis) {
    this.receiveTimeoutMillis = receiveTimeoutMillis;
  }

  public long getInitialDelaySeconds() {
    return initialDelaySeconds;
  }

  public void setInitialDelaySeconds(long initialDelaySeconds) {
    this.initialDelaySeconds = initialDelaySeconds;
  }

  public long getDelaySeconds() {
    return delaySeconds;
  }

  public void setDelaySeconds(long delaySeconds) {
    this.delaySeconds = delaySeconds;
  }

  /**
   * Read JMS Messages.
   */
  public void run() {
    log.debug("Entering message processing loop for consumer {}", destinationName);

    if (scheduled) {
      if (false == createConnection(false)) {
        return;
      }
      try {
          createConsumer();
          startConnection();

          while (scheduled && isConnectionStarted()) {
            try {
              Message message = consumer.receive(receiveTimeoutMillis);
              if (message != null) {
                sendMessageToSplunk(message);
              }
            } catch (JMSException jmsEx) {
              Throwable cause = jmsEx.getCause();
              if (cause != null && cause instanceof InterruptedException) {
                // If we're still supposed to be scheduled, re-throw the exception; otherwise just log it
                if (scheduled) {
                  throw jmsEx;
                } else {
                  cleanup(false);
                  if (log.isDebugEnabled()) {
                    String debugMessage = String.format("Consumer.receive(%d) call interrupted in processing loop for %s - stopping consumer", receiveTimeoutMillis, destinationName);
                    log.debug(debugMessage, jmsEx);
                  }
                }
              }
            }
          }
      } catch (Exception ex) {
        cleanup(false);
        if (scheduled) {
          String warningMessage = String.format("Exception encountered in processing loop for %s - restart will be attempted in %d seconds", destinationName, delaySeconds);
          log.info(warningMessage, ex);
        } else {
          String infoMessage = String.format("Exception encountered in processing loop for %s - stopping consumer", destinationName);
          log.warn(infoMessage, ex);
        }
      } finally {
        log.info("JMS consumer for {} stopped", destinationName);
        stopConnection();
      }
    }

  }

}
