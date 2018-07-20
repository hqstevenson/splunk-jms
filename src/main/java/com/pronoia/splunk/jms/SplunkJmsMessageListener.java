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

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import com.pronoia.splunk.eventcollector.SplunkMDCHelper;


/**
 * Receives JMS Messages from an ActiveMQ broker in the same JVM and delivers them to Splunk using the HTTP Event
 * Collector.
 */
public class SplunkJmsMessageListener extends SplunkJmsConsumerSupport implements MessageListener, ExceptionListener {

    public SplunkJmsMessageListener(String destinationName) {
        this(destinationName, false);
    }

    public SplunkJmsMessageListener(String destinationName, boolean useTopic) {
        super(destinationName, useTopic);
    }

    /**
     * Start the JMS QueueReceiver.
     *
     * TODO:  Fixup error handling
     */
    public void start() {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            log.info("Starting MessageListener for {}", destinationName);
            verifyConfiguration();

            createConnection(true);
            createConsumer();

            try {
                log.trace("Registering the MessageListener for Consumer for {}", destinationName);
                consumer.setMessageListener(this);
            } catch (JMSException jmsEx) {
                cleanup(false);
                String errorMessage = String.format("Exception encountered registering JMS MessageListener {destinationName name ='%s'}", destinationName);
                log.error(errorMessage, jmsEx);
                throw new IllegalStateException(errorMessage, jmsEx);
            }

            try {
                // This will throw a JMSException with a ConnectException cause when the connection cannot be made to a TCP URL
                log.trace("Registering the JMS ExceptionListener for {}", destinationName);
                connection.setExceptionListener(this);
            } catch (JMSException jmsEx) {
                cleanup(false);
                String errorMessage = String.format("Exception encountered registering the exception listener {destinationName name ='%s'}", destinationName);
                log.error(errorMessage, jmsEx);
                throw new IllegalStateException(errorMessage, jmsEx);
            } catch (Throwable unexpectedEx) {
                cleanup(false);
                String errorMessage = String.format("Exception encountered registering the exception listener  {destinationName name ='%s'}", destinationName);
                log.error(errorMessage, unexpectedEx);
                throw new IllegalStateException(errorMessage, unexpectedEx);
            }

            startConnection();

            log.info("MessageListener for {} started", destinationName);
        }
    }

    public void stop() {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            log.info("Stopping MessageListener for {}", destinationName);
            stopConnection();
        }
    }

    @Override
    public void onMessage(Message message) {
        // TODO:  The error handling needs to be worked over - basic logging probably isn't enough
        try (SplunkMDCHelper helper = createMdcHelper()) {
            log.debug("onMessage called");
            if (message != null) {
                sendMessageToSplunk(message);
            }
        }
    }

    @Override
    public void onException(JMSException jmsEx) {
        // TODO: Figure this one out
        try (SplunkMDCHelper helper = createMdcHelper()) {
            log.error("JMSException Encountered - stopping Listener", jmsEx);

            // We'll get an EOFException cause when the connection drops
            cleanup(false);
        }
    }
}
