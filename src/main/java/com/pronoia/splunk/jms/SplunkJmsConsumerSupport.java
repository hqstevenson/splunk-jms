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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorClient;
import com.pronoia.splunk.eventcollector.EventCollectorInfo;
import com.pronoia.splunk.eventcollector.EventDeliveryException;
import com.pronoia.splunk.eventcollector.EventDeliveryHttpException;
import com.pronoia.splunk.eventcollector.SplunkMDCHelper;
import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;

import java.io.Serializable;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
    volatile Date connectionStartTime;
    volatile Date connectionStopTime;
    volatile Date lastMessageTime;
    volatile long consumedMessageCount;

    EventBuilder<Message> splunkEventBuilder;

    AtomicBoolean skipNextMessage = new AtomicBoolean(false);

    Set<Integer> consumedHttpStatusCodes;
    Set<Integer> consumedSplunkStatusCodes;

    {
        consumedHttpStatusCodes = new HashSet<>();
        consumedHttpStatusCodes.add(200);

        consumedSplunkStatusCodes = new HashSet<>();
        consumedSplunkStatusCodes.add(0); // Success
        consumedSplunkStatusCodes.add(5); // No Data
//        consumedSplunkStatusCodes.add(6); // Invalid data format
        consumedSplunkStatusCodes.add(12); // Event field is required
        consumedSplunkStatusCodes.add(13); // Event field cannot be blank
    }

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

    public Date getConnectionStartTime() {
        return connectionStartTime;
    }

    public Date getLastMessageTime() {
        return lastMessageTime;
    }

    public long getConsumedMessageCount() {
        return consumedMessageCount;
    }

    public Date getConnectionStopTime() {
        return connectionStartTime;
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

    public boolean getSkipNextMessage() {
        return  (skipNextMessage != null) ? skipNextMessage.get() : false;
    }

    public void setSkipNextMessage(boolean skipNextMessage) {
        if (this.skipNextMessage != null) {
            this.skipNextMessage.set(skipNextMessage);
        } else {
            this.skipNextMessage = new AtomicBoolean(skipNextMessage);
        }
    }

    public void skipNextMessage() {
        this.setSkipNextMessage(true);
    }

    public boolean isConsumedHttpStatusCode(int httpStatusCode) {

        return hasConsumedHttpStatusCodes() &&  consumedHttpStatusCodes.contains(httpStatusCode);
    }

    public boolean hasConsumedHttpStatusCodes() {
        return consumedHttpStatusCodes != null && !consumedHttpStatusCodes.isEmpty();
    }

    public Set<Integer> getConsumedHttpStatusCodes() {
        return consumedHttpStatusCodes;
    }

    public void setConsumedHttpStatusCodes(Set<Integer> consumedHttpStatusCodes) {
        if (consumedHttpStatusCodes != null) {
            if (this.consumedHttpStatusCodes == null) {
                this.consumedHttpStatusCodes = new HashSet<>();
            } else {
                this.consumedHttpStatusCodes.clear();
            }
            this.consumedHttpStatusCodes.addAll(consumedHttpStatusCodes);
        }
    }

    public boolean isConsumedSplunkStatusCode(int splunkStatusCode) {

        return hasConsumedSplunkStatusCodes() &&  consumedSplunkStatusCodes.contains(splunkStatusCode);
    }

    public boolean hasConsumedSplunkStatusCodes() {
        return consumedSplunkStatusCodes != null && !consumedSplunkStatusCodes.isEmpty();
    }

    public Set<Integer> getConsumedSplunkStatusCodes() {
        return consumedSplunkStatusCodes;
    }

    public void setConsumedSplunkStatusCodes(Set<Integer> consumedSplunkStatusCodes) {
        if (consumedSplunkStatusCodes != null) {
            if (this.consumedSplunkStatusCodes == null) {
                this.consumedSplunkStatusCodes = new HashSet<>();
            } else {
                this.consumedSplunkStatusCodes.clear();
            }
            this.consumedSplunkStatusCodes.addAll(consumedSplunkStatusCodes);
        }
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
                connectionStartTime = new Date();
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
                if (skipNextMessage != null && skipNextMessage.get()) {
                    String jmsMessageId = "unknown";
                    try {
                        jmsMessageId = message.getJMSMessageID();
                    } catch (JMSException getMessageIdEx) {
                        log.warn("Failed to retrieve value of JMSMessageID: {}", message, getMessageIdEx);
                    }
                    log.info("Skipping message: JMSMessageID={} body={}", jmsMessageId, getMessageBody(message));
                    commitMessage(message);
                    skipNextMessage.set(false);
                } else {
                    String eventBody = splunkEventBuilder.eventBody(message).build(splunkClient);

                    try {
                        splunkClient.sendEvent(eventBody);
                        commitMessage(message);
                    } catch (EventDeliveryHttpException eventDeliveryHttpEx) {
                        if (isConsumedSplunkStatusCode(eventDeliveryHttpEx.getSplunkStatusCode()) || isConsumedHttpStatusCode(eventDeliveryHttpEx.getHttpStatusCode())) {
                            log.warn("Ignoring exception encountered processing event - event body will follow at DEBUG level: code={} text={}",
                                    eventDeliveryHttpEx.getSplunkStatusCode(), eventDeliveryHttpEx.getSplunkStatusMessage());
                            log.debug("Ignoring event: {}", eventBody);

                            commitMessage(message);
                        } else {
                            log.warn("Splunk failed to process message - rolling-back session: response_code={}, response_reason={}, status_code={}, status_message={}",
                                    eventDeliveryHttpEx.getHttpStatusCode(), eventDeliveryHttpEx.getHttpReasonPhrase(),
                                    eventDeliveryHttpEx.getSplunkStatusCode(), eventDeliveryHttpEx.getSplunkStatusMessage(),
                                    eventDeliveryHttpEx);
                            rollbackMessage(message);
                        }
                    } catch (EventDeliveryException eventDeliveryEx) {
                        log.warn("Failed to deliver message to Splunk - rolling-back session", eventDeliveryEx);
                        rollbackMessage(message);
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
            if (connection != null) {
                try {
                    connection.stop();
                } catch (Exception jmsEx) {
                    if (logExceptions) {
                        String logMessage = String.format("Exception encountered stopping JMS Connection {destinationName name ='%s'}", destinationName);
                        log.warn(logMessage, jmsEx);
                    }
                } finally {
                    connectionStarted = false;
                    connectionStopTime = new Date();
                }
            }

            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception jmsEx) {
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
                } catch (Exception jmsEx) {
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
                } catch (Exception jmsEx) {
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

    void commitMessage(Message message) {
        try {
            session.commit();
            ++consumedMessageCount;
            lastMessageTime = new Date();
        } catch (JMSException jmsAcknowledgeEx) {
            String messageID= "<unknown>";
            try {
                messageID = message.getJMSMessageID();
            } catch (JMSException getMessageIdEx) {
                // Eat this - should never happen
            }
            log.error("Failed to acknowledge and commit JMS Message: JMSMessageID=%s ", messageID, jmsAcknowledgeEx);
        }
    }

    void rollbackMessage(Message message) {
        try {
            session.rollback();
        } catch (JMSException jmsRollbackEx) {
            String messageID= "<unknown>";
            try {
                messageID = message.getJMSMessageID();
            } catch (JMSException getMessageIdEx) {
                // Eat this - should never happen
            }
            log.error("Failed to rollback JMS Message: JMSMessageID=%s ", messageID, jmsRollbackEx);
        }
    }

    Object getMessageBody(Message jmsMessage) {
        Object messageBody;
        if (jmsMessage != null) {
            if (jmsMessage instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) jmsMessage;
                try {
                    messageBody = textMessage.getText();
                } catch (JMSException getTextEx) {
                    log.warn("Exception encountered reading body from TextMessage ", getTextEx);
                    messageBody = "<unknown TextMessage body>";
                }
            } else if (jmsMessage instanceof BytesMessage) {
                BytesMessage bytesMessage = (BytesMessage) jmsMessage;
                try {
                    long bodyLength = 0;
                    bodyLength = bytesMessage.getBodyLength();
                    if (bodyLength > 0) {
                        try {
                            byte[] bodyBytes = new byte[(int) bodyLength];
                            bytesMessage.readBytes(bodyBytes);
                            messageBody = new String(bodyBytes);
                        } catch (JMSException readBytesEx) {
                            log.warn("Exception encountered reading byte[] from BytesMessage", readBytesEx);
                            messageBody = "<unknown BytesMessage body>";
                        }
                    } else {
                        messageBody = "<empty BytesMessage body>";
                    }
                } catch (JMSException getBodyLengthEx) {
                    log.warn("Exception encountered getting byte[] length from BytesMessage", getBodyLengthEx);
                    messageBody = "<unknown BytesMessage> body";
                }
            } else if (jmsMessage instanceof MapMessage) {
                MapMessage mapMessage = (MapMessage) jmsMessage;
                try {
                    Enumeration<String> keys = mapMessage.getMapNames();
                    if (keys != null && keys.hasMoreElements()) {
                        Map<String, Object> mapMessageBody = new LinkedHashMap<>();
                        while (keys.hasMoreElements()) {
                            String key = keys.nextElement();
                            try {
                                String value = mapMessage.getString(key);
                                if (value != null) {
                                    mapMessageBody.put(key, value);
                                }
                            } catch (JMSException getStringPropertyEx) {
                                log.warn("Exception encountered retrieving value for key '{}' from MapMessage", key, getStringPropertyEx);
                            }
                        }
                        if (mapMessageBody != null && !mapMessageBody.isEmpty()) {
                            messageBody = mapMessageBody;
                        } else {
                            messageBody = "<empty MapMessage body>";
                        }
                    } else {
                        log.warn("Empty or null Enumeration returned by MapMessage.getMapNames()");
                        messageBody = "<empty MapMessage body>";
                    }
                } catch (JMSException getMapNamesEx) {
                    log.warn("Exception encountered retrieving keys from MapMessage", getMapNamesEx);
                    messageBody = "<unknown MapMessage body>";
                }
            } else if (jmsMessage instanceof ObjectMessage) {
                ObjectMessage objectMessage = (ObjectMessage) jmsMessage;
                try {
                    Serializable objectBody = objectMessage.getObject();
                    if (objectBody != null) {
                        String objectBodyString = objectBody.toString();
                        if (!objectBodyString.isEmpty()) {
                            messageBody = objectBodyString;
                        } else {
                            messageBody = "<empty ObjectMessage body>";
                        }
                    } else {
                        messageBody = "<null ObjectMessage body>";
                    }
                } catch (JMSException getObjectEx) {
                    log.warn("Exception encountered reading Object from ObjectMessage", getObjectEx);
                    messageBody = "<unknown ObjectMessage body>";
                }
            } else if (jmsMessage instanceof StreamMessage) {
                log.warn("Unsupported JMS Message type: {}", jmsMessage.getClass().getName());
                messageBody = "<unsupported StreamMessage body>";
            } else {
                log.warn("Unknown JMS Message type: {}", jmsMessage.getClass().getName());
                messageBody = String.format("<unknown %s body>", jmsMessage.getClass().getName());
            }
        } else {
            messageBody = "<null Message>";
        }

        return messageBody;
    }
}
