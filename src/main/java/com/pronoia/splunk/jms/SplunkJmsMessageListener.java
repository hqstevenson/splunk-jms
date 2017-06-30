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

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorClient;
import com.pronoia.splunk.eventcollector.EventDeliveryException;
import com.pronoia.splunk.jms.builder.JmsMessageEventBuilder;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives JMS Messages from an ActiveMQ broker in the same JVM and delivers them to Splunk using the HTTP Event
 * Collector.
 *
 * <p>If the broker name isn't configured, the broker is located using JMX - the first broker returned by the JMX
 * queury will be used.
 */
public class SplunkJmsMessageListener implements MessageListener, ExceptionListener {
  Logger log = LoggerFactory.getLogger(this.getClass());

  ConnectionFactory connectionFactory;

  EventCollectorClient splunkClient;

  Connection connection;
  Session session;
  MessageConsumer consumer;

  String destinationName;

  boolean useQueue = true;
  boolean running = false;

  EventBuilder<Message> messageEventBuilder;

  public boolean hasConnectionFactory() {
    return connectionFactory != null;

  }

  public ConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  public void setConnectionFactory(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  public boolean hasDestinationName() {
    return destinationName != null;
  }

  public String getDestinationName() {
    return destinationName;
  }

  public void setDestinationName(String destinationName) {
    this.destinationName = destinationName;
  }

  public boolean isUseQueue() {
    return useQueue;
  }

  public void setUseQueue(boolean useQueue) {
    this.useQueue = useQueue;
  }

  public void useQueue() {
    this.setUseQueue(true);
  }

  public void useFalse() {
    this.setUseQueue(false);
  }

  public boolean hasSplunkClient() {
    return splunkClient != null;
  }

  public EventCollectorClient getSplunkClient() {
    return splunkClient;
  }

  public void setSplunkClient(EventCollectorClient splunkClient) {
    this.splunkClient = splunkClient;
  }

  public boolean hasMessageEventBuilder() {
    return messageEventBuilder != null;
  }

  public EventBuilder<Message> getMessageEventBuilder() {
    return messageEventBuilder;
  }

  public void setMessageEventBuilder(EventBuilder<Message> messageEventBuilder) {
    this.messageEventBuilder = messageEventBuilder;
  }

  public void verifyConfiguration() {
    if (!hasConnectionFactory()) {
      throw new IllegalStateException("JMS Connection Factory must be specified");
    }

    if (!hasDestinationName()) {
      throw new IllegalStateException("JMS Destination Name must be specified");
    }

    if (!hasSplunkClient()) {
      throw new IllegalStateException("Splunk Client must be specified");
    }

    if (!hasMessageEventBuilder()) {
      log.info("A Splunk event builder was not specified - the default event builder will be used");
      messageEventBuilder = new JmsMessageEventBuilder();
    }
  }

  public boolean isRunning() {
    return running;
  }

  /**
   * Start the JMS QueueReceiver.
   *
   * TODO:  Fixup error handling
   */
  public void start() {
    verifyConfiguration();

    try {
      // This will throw a JMSException with a ConnectException cause when the connection cannot be made to a TCP URL
      connection = connectionFactory.createConnection();
      connection.setExceptionListener(this);
    } catch (JMSException jmsEx) {
      String logMessage = String.format("Exception encountered creating JMS Connection {destinationName name ='%s'}", destinationName);
      throw new IllegalStateException(logMessage, jmsEx);
    }

    try {
      // This will throw a JMSSecurityException when the user or password is invalid
      session = connection.createSession(true, Session.SESSION_TRANSACTED);
    } catch (JMSException jmsEx) {
      String logMessage = String.format("Exception encountered creating JMS Session {destinationName name ='%s'}", destinationName);
      throw new IllegalStateException(logMessage, jmsEx);
    }

    try {
      consumer = session.createConsumer(createDestination());
      consumer.setMessageListener(this);
    } catch (JMSException jmsEx) {
      String logMessage = String.format("Exception encountered creating JMS MessageConsumer {destinationName name ='%s'}", destinationName);
      throw new IllegalStateException(logMessage, jmsEx);
    }

    try {
      connection.start();
    } catch (JMSException jmsEx) {
      String logMessage = String.format("Exception encountered starting JMS Connection {destinationName name ='%s'}", destinationName);
      throw new IllegalStateException(logMessage, jmsEx);
    }

    running = true;
  }

  /**
   * Stop the JMS QueueReceiver.
   */
  public void stop() {
    this.cleanup(true);
  }

  /**
   * Cleanup all the JMS Resources.
   */
  void cleanup(boolean logExceptions) {
    if (connection != null) {
      try {
        connection.stop();
      } catch (JMSException jmsEx) {
        if (logExceptions) {
          String logMessage = String.format("Exception encountered stopping JMS Connection {destinationName name ='%s'}", destinationName);
          log.warn(logMessage, jmsEx);
        }
      }
    }

    if (consumer != null) {
      try {
        consumer.close();
      } catch (JMSException jmsEx) {
        if (logExceptions) {
          String logMessage = String.format("Exception encountered closing JMS MessageConsumer {destinationName name ='%s'}", destinationName);
          log.warn(logMessage, jmsEx);
        }
      } finally {
        consumer = null;
      }
    }

    if (session != null) {
      try {
        session.close();
      } catch (JMSException jmsEx) {
        if (logExceptions) {
          String logMessage = String.format("Exception encountered closing JMS Session {destinationName name ='%s'}", destinationName);
          log.warn(logMessage, jmsEx);
        }
      } finally {
        session = null;
      }
    }

    if (connection != null) {
      try {
        connection.close();
      } catch (JMSException jmsEx) {
        if (logExceptions) {
          String logMessage = String.format("Exception encountered closing JMS Connection {destinationName name ='%s'}", destinationName);
          log.warn(logMessage, jmsEx);
        }
      } finally {
        connection = null;
      }
    }

    running = false;
  }

  @Override
  public void onMessage(Message message) {
    // TODO:  The error handling needs to be worked over - basic logging probably isn't enough
    if (message != null) {
      if (log.isDebugEnabled()) {
        try {
          String jmsMessageId = message.getJMSMessageID();
          long jmsTimestamp = message.getJMSTimestamp();
          log.debug("onMessage received JMSMessageID={} JMSTimestamp={}", jmsMessageId, jmsTimestamp);
        } catch (JMSException jmsEx) {
          log.warn("Failed to retrieve value of JMSMessageID and JMSTimestamp for onMessage received log entry");
        }
      }
      messageEventBuilder.setEvent(message);

      try {
        splunkClient.sendEvent(messageEventBuilder.build());
        try {
          message.acknowledge();
          session.commit();
        } catch (JMSException jmsAcknowledgeEx) {
          try {
            String logMessage = String.format("Failed to acknowledge and commit JMS Message JMSMessageID={} ", message.getJMSMessageID());
            log.error(logMessage, jmsAcknowledgeEx);
          } catch (JMSException getMessageIdEx) {
            log.error("Failed to acknowledge JMS Message", jmsAcknowledgeEx);
          }
        }
      } catch (EventDeliveryException eventDeliveryEx) {
        log.warn("Failed to deliver message to Splunk - rolling-back session", eventDeliveryEx);
        try {
          session.rollback();
        } catch (JMSException jmsRollbackEx) {
          log.error("Failed to rollback JMS Session", jmsRollbackEx);
        }
      }
    }
  }

  @Override
  public void onException(JMSException jmsEx) {
    // TODO: Figure this one out
    log.error("JMSException Encountered - stopping Listener", jmsEx);

    // We'll get an EOFException cause when the connection drops
    cleanup(false);
  }

  protected Destination createDestination() throws JMSException {
    return useQueue ? session.createQueue(destinationName) : session.createTopic(destinationName);
  }
}
