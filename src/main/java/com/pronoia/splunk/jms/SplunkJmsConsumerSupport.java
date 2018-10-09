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
package com.pronoia.splunk.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorClient;
import com.pronoia.splunk.eventcollector.EventDeliveryException;
import com.pronoia.splunk.eventcollector.SplunkMDCHelper;
import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


/**
 * Receives JMS Messages from an ActiveMQ broker in the same JVM and delivers them to Splunk using the HTTP Event Collector.
 */
public class SplunkJmsConsumerSupport {
    protected Logger log = LoggerFactory.getLogger(this.getClass());
    String destinationName;
    boolean useTopic;
    ConnectionFactory connectionFactory;
    EventCollectorClient splunkClient;
    Connection connection;
    Session session;
    MessageConsumer consumer;
    volatile boolean connectionStarted;


    EventBuilder<Message> splunkEventBuilder;

    public SplunkJmsConsumerSupport(String destinationName) {
        this.destinationName = destinationName;
    }

    public SplunkJmsConsumerSupport(String destinationName, boolean useTopic) {
        this.destinationName = destinationName;
        this.useTopic = useTopic;
    }

    public boolean hasDestinationName() {
        return destinationName != null && !destinationName.isEmpty();
    }

    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    public boolean hasConnectionFactory() {
        return connectionFactory != null;

    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public boolean isConnectionStarted() {
        return connectionStarted;
    }

    public void setConnectionStarted(boolean connectionStarted) {
        this.connectionStarted = connectionStarted;
    }

    public boolean isUseTopic() {
        return useTopic;
    }

    public void setUseTopic(boolean useTopic) {
        this.useTopic = useTopic;
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
        return splunkEventBuilder != null;
    }

    public EventBuilder<Message> getSplunkEventBuilder() {
        return splunkEventBuilder;
    }

    public void setSplunkEventBuilder(EventBuilder<Message> splunkEventBuilder) {
        this.splunkEventBuilder = splunkEventBuilder;
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
            splunkEventBuilder = new JmsMessageEventBuilder();
        }
    }

    protected boolean createConnection(boolean throwException) {
        cleanup(false);
        try (SplunkMDCHelper helper = createMdcHelper()) {
            log.debug("Establishing connection for {}", destinationName);

            try {
                log.trace("Creating JMS Connection for {}", destinationName);
                ConnectionFactory tmpConnectionFactory = getConnectionFactory();
                if (tmpConnectionFactory != null) {
                    connection = tmpConnectionFactory.createConnection();
                    if (throwException && connection == null) {
                        throw new IllegalStateException("Failed to create connection from connection factory");
                    }
                } else if (throwException) {
                    throw new IllegalStateException("getConnectionFactory returned null");
                }
            } catch (JMSException jmsEx) {
                cleanup(false);
                String errorMessage = String.format("Exception encountered creating JMS Connection {destinationName name ='%s'}", destinationName);
                if (throwException) {
                    throw new IllegalStateException(errorMessage, jmsEx);
                }
                log.info(errorMessage, jmsEx);
            }
        }
        return connection != null;
    }

    public boolean hasConnection() {
        return connection != null;
    }

    public boolean hasSession() {
        return session != null;
    }

    public boolean hasConsumer() {
        return consumer != null;
    }

    protected Connection getConnection() {
        return connection;
    }

    protected Session getSession() {
        return session;
    }

    protected MessageConsumer getConsumer() {
        return consumer;
    }

    /**
     * Connect to the broker and create the JMS Consumer.
     *
     * TODO:  Fixup error handling
     */
    protected void createConsumer() {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            try {
                // This will throw a JMSSecurityException when the user or password is invalid
                log.trace("Creating JMS Session for {}", destinationName);
                session = connection.createSession(true, -1);
            } catch (JMSException jmsEx) {
                cleanup(false);
                String errorMessage = String.format("Exception encountered creating JMS Session {destinationName name ='%s'}", destinationName);
                log.error(errorMessage, jmsEx);
                throw new IllegalStateException(errorMessage, jmsEx);
            }

            try {
                log.trace("Creating JMS Consumer for {}", destinationName);
                consumer = session.createConsumer(createDestination());
            } catch (JMSException jmsEx) {
                cleanup(false);
                String errorMessage = String.format("Exception encountered creating JMS MessageConsumer {destinationName name ='%s'}", destinationName);
                log.error(errorMessage, jmsEx);
                throw new IllegalStateException(errorMessage, jmsEx);
            }
        }
    }

    protected void startConnection() {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            try {
                log.trace("Starting JMS connection for {}", destinationName);
                connection.start();
                connectionStarted = true;
            } catch (JMSException jmsEx) {
                cleanup(false);
                String errorMessage = String.format("Exception encountered starting JMS Connection {destinationName name ='%s'}", destinationName);
                log.error(errorMessage, jmsEx);
                throw new IllegalStateException(errorMessage, jmsEx);
            }

            log.debug("Connection for {} started", destinationName);
        }
    }

    /**
     * Stop the JMS QueueReceiver.
     */
    protected void stopConnection() {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            log.debug("Stopping Connection for {}", destinationName);
            this.cleanup(true);
        }
    }

    protected Destination createDestination() throws JMSException {
        return useTopic ? session.createTopic(destinationName) : session.createQueue(destinationName);
    }

    protected void sendMessageToSplunk(Message message) {
        // TODO:  The error handling needs to be worked over - basic logging probably isn't enough
        try (SplunkMDCHelper helper = createMdcHelper()) {
            log.debug("sendMessageToSplunk called");
            if (message != null) {
                if (log.isDebugEnabled()) {
                    try {
                        String jmsMessageId = message.getJMSMessageID();
                        long jmsTimestamp = message.getJMSTimestamp();
                        log.debug("sendMessageToSplunk received JMSMessageID={} JMSTimestamp={}", jmsMessageId, jmsTimestamp);
                    } catch (JMSException jmsEx) {
                        log.warn("Failed to retrieve value of JMSMessageID and JMSTimestamp for sendMessageToSplunk received log entry");
                    }
                }
                String eventBody = splunkEventBuilder.eventBody(message).build();

                try {
                    splunkClient.sendEvent(eventBody);
                    try {
                        // message.acknowledge();
                        session.commit();
                    } catch (JMSException jmsAcknowledgeEx) {
                        try {
                            String logMessage = String.format("Failed to acknowledge and commit JMS Message JMSMessageID=%s ", message.getJMSMessageID());
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
    }

    /**
     * Cleanup all the JMS Resources.
     */
    void cleanup(boolean logExceptions) {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            connectionStarted = false;

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
        }
    }


    protected SplunkMDCHelper createMdcHelper() {
        return new JmsMessageListenerMDCHelper();
    }

    class JmsMessageListenerMDCHelper extends SplunkMDCHelper {
        public static final String MDC_JMS_DESTINATION = "splunk.jms.source";

        JmsMessageListenerMDCHelper() {
            addEventBuilderValues(splunkEventBuilder);
            saveContextMap();
            MDC.put(MDC_JMS_DESTINATION, destinationName);
        }
    }

}
