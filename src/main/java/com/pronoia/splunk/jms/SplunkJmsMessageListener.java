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
 */
public class SplunkJmsMessageListener implements MessageListener, ExceptionListener {
  Logger log = LoggerFactory.getLogger(this.getClass());

  ConnectionFactory connectionFactory;

  EventCollectorClient splunkClient;

  Connection connection;
  Session session;
  MessageConsumer consumer;

  String destinationName;

  boolean useTopic = false;
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

  public boolean isUseTopic() {
    return useTopic;
  }

  public void setUseTopic(boolean useTopic) {
    this.useTopic = useTopic;
  }

  public void useQueue() {
    this.setUseTopic(false);
  }

  public void useTopic() {
    this.setUseTopic(true);
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
    log.info("Starting MessageListener for {}", destinationName);
    verifyConfiguration();

    try {
      log.trace("Creating JMS Connection for {}", destinationName);
      connection = connectionFactory.createConnection();
    } catch (JMSException jmsEx) {
      String errorMessage = String.format("Exception encountered creating JMS Connection {destinationName name ='%s'}", destinationName);
      log.error(errorMessage, jmsEx);
      throw new IllegalStateException(errorMessage, jmsEx);
    } catch (Throwable unexpectedEx) {
      String errorMessage = String.format("Unexpected exception encountered creating JMS Connection {destinationName name ='%s'}", destinationName);
      log.error(errorMessage, unexpectedEx);
      throw new IllegalStateException(errorMessage, unexpectedEx);
    }

    for (int i=0; i<5; ++i) {
      try {
        // This will throw a JMSSecurityException when the user or password is invalid
        log.trace("Creating JMS Session for {} - attempt {}", destinationName, i);
        session = connection.createSession(true, -1);
        if (session != null) {
          log.info("Connected on attempt {}", i);
          break;
        } else {
          log.warn("Connection failed - sleeping before reconnect on attempt {}", i);
          Thread.sleep(5000);
        }
      } catch (JMSException jmsEx) {
        String errorMessage = String.format("Exception encountered creating JMS Session {destinationName name ='%s'} - attempt %d", destinationName, i);
        log.error(errorMessage, jmsEx);
        log.warn("Connection failed - sleeping before reconnect on attempt {}", i);
        try {
          Thread.sleep(15000);
        } catch (InterruptedException interruptedEx) {
          log.info("Sleep for reconnect interrupted", interruptedEx);
        }
        // throw new IllegalStateException(errorMessage, jmsEx);
      } catch (Throwable unexpectedEx) {
        String errorMessage = String.format("Unexpected exception encountered creating JMS Session {destinationName name ='%s'} - attempt %d", destinationName, i);
        log.error(errorMessage, unexpectedEx);
        // throw new IllegalStateException(errorMessage, unexpectedEx);
      }
    }

    try {
      log.trace("Creating JMS Consumer for {}", destinationName);
      consumer = session.createConsumer(createDestination());
      consumer.setMessageListener(this);
    } catch (JMSException jmsEx) {
      String errorMessage = String.format("Exception encountered creating JMS MessageConsumer {destinationName name ='%s'}", destinationName);
      log.error(errorMessage, jmsEx);
      throw new IllegalStateException(errorMessage, jmsEx);
    }

    try {
      // This will throw a JMSException with a ConnectException cause when the connection cannot be made to a TCP URL
      log.trace("Registering the exception listener for {}", destinationName);
      connection.setExceptionListener(this);
    } catch (JMSException jmsEx) {
      String errorMessage = String.format("Exception encountered registering the exception listener {destinationName name ='%s'}", destinationName);
      log.error(errorMessage, jmsEx);
      throw new IllegalStateException(errorMessage, jmsEx);
    } catch (Throwable unexpectedEx) {
      String errorMessage = String.format("Exception encountered registering the exception listener  {destinationName name ='%s'}", destinationName);
      log.error(errorMessage, unexpectedEx);
      throw new IllegalStateException(errorMessage, unexpectedEx);
    }

    try {
      log.trace("Starting JMS connection for {}", destinationName);
      connection.start();
    } catch (JMSException jmsEx) {
      String errorMessage = String.format("Exception encountered starting JMS Connection {destinationName name ='%s'}", destinationName);
      log.error(errorMessage, jmsEx);
      throw new IllegalStateException(errorMessage, jmsEx);
    }

    log.info("MessageListener for {} started", destinationName);

    running = true;
  }

  /**
   * Stop the JMS QueueReceiver.
   */
  public void stop() {
    log.info("Stopping MessageListener for {}", destinationName);
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
    log.debug("onMessage called");
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
          // message.acknowledge();
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
    return useTopic ? session.createTopic(destinationName) : session.createQueue(destinationName);
  }
}
