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
package com.pronoia.splunk.jms.eventbuilder;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorInfo;
import com.pronoia.splunk.eventcollector.SplunkMDCHelper;
import com.pronoia.splunk.eventcollector.eventbuilder.EventBuilderSupport;
import com.pronoia.splunk.eventcollector.eventbuilder.JacksonEventBuilderSupport;


public class JmsMessageEventBuilder extends JacksonEventBuilderSupport<Message> {
    public static final String JMS_DESTINATION = "JMSDestination";
    public static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_PRIORITY = "JMSPriority";
    public static final String JMS_MESSAGE_ID = "JMSMessageID";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";
    public static final String JMS_CORRELATION_ID = "JMSCorrelationID";
    public static final String JMS_REPLY_TO = "JMSReplyTo";
    public static final String JMS_TYPE = "JMSType";
    public static final String JMS_REDELIVERED = "JMSRedelivered";

    boolean includeJmsDestination = true;
    boolean includeJmsDeliveryMode = true;
    boolean includeJmsExpiration = true;
    boolean includeJmsPriority = true;
    boolean includeJmsMessageId = true;
    boolean includeJmsTimestamp = true;
    boolean includeJmsCorrelationId = true;
    boolean includeJmsReplyTo = true;
    boolean includeJmsType = true;
    boolean includeJmsRedelivered = true;

    boolean includeJmsProperties = true;

    String hostProperty;
    String indexProperty;
    String sourceProperty;
    String sourcetypeProperty;
    String timestampProperty;

    Map<String, String> propertyNameReplacements;

    public boolean isIncludeJmsDestination() {
        return includeJmsDestination;
    }

    public void setIncludeJmsDestination(boolean includeJmsDestination) {
        this.includeJmsDestination = includeJmsDestination;
    }

    public boolean isIncludeJmsDeliveryMode() {
        return includeJmsDeliveryMode;
    }

    public void setIncludeJmsDeliveryMode(boolean includeJmsDeliveryMode) {
        this.includeJmsDeliveryMode = includeJmsDeliveryMode;
    }

    public boolean isIncludeJmsExpiration() {
        return includeJmsExpiration;
    }

    public void setIncludeJmsExpiration(boolean includeJmsExpiration) {
        this.includeJmsExpiration = includeJmsExpiration;
    }

    public boolean isIncludeJmsPriority() {
        return includeJmsPriority;
    }

    public void setIncludeJmsPriority(boolean includeJmsPriority) {
        this.includeJmsPriority = includeJmsPriority;
    }

    public boolean isIncludeJmsMessageId() {
        return includeJmsMessageId;
    }

    public void setIncludeJmsMessageId(boolean includeJmsMessageId) {
        this.includeJmsMessageId = includeJmsMessageId;
    }

    public boolean isIncludeJmsTimestamp() {
        return includeJmsTimestamp;
    }

    public void setIncludeJmsTimestamp(boolean includeJmsTimestamp) {
        this.includeJmsTimestamp = includeJmsTimestamp;
    }

    public boolean isIncludeJmsCorrelationId() {
        return includeJmsCorrelationId;
    }

    public void setIncludeJmsCorrelationId(boolean includeJmsCorrelationId) {
        this.includeJmsCorrelationId = includeJmsCorrelationId;
    }

    public boolean isIncludeJmsReplyTo() {
        return includeJmsReplyTo;
    }

    public void setIncludeJmsReplyTo(boolean includeJmsReplyTo) {
        this.includeJmsReplyTo = includeJmsReplyTo;
    }

    public boolean isIncludeJmsType() {
        return includeJmsType;
    }

    public void setIncludeJmsType(boolean includeJmsType) {
        this.includeJmsType = includeJmsType;
    }

    public boolean isIncludeJmsRedelivered() {
        return includeJmsRedelivered;
    }

    public void setIncludeJmsRedelivered(boolean includeJmsRedelivered) {
        this.includeJmsRedelivered = includeJmsRedelivered;
    }

    public boolean isIncludeJmsProperties() {
        return includeJmsProperties;
    }

    public void setIncludeJmsProperties(boolean includeJmsProperties) {
        this.includeJmsProperties = includeJmsProperties;
    }

    public boolean hasHostProperty() {
        return hostProperty != null && !hostProperty.isEmpty();
    }

    public String getHostProperty() {
        return hostProperty;
    }

    public void setHostProperty(String hostProperty) {
        this.hostProperty = hostProperty;
    }

    public boolean hasIndexProperty() {
        return indexProperty != null && !indexProperty.isEmpty();
    }

    public String getIndexProperty() {
        return indexProperty;
    }

    public void setIndexProperty(String indexProperty) {
        this.indexProperty = indexProperty;
    }

    public boolean hasSourceProperty() {
        return sourceProperty != null && !sourceProperty.isEmpty();
    }

    public String getSourceProperty() {
        return sourceProperty;
    }

    public void setSourceProperty(String sourceProperty) {
        this.sourceProperty = sourceProperty;
    }

    public boolean hasSourcetypeProperty() {
        return sourcetypeProperty != null && !sourcetypeProperty.isEmpty();
    }

    public String getSourcetypeProperty() {
        return sourcetypeProperty;
    }

    public void setSourcetypeProperty(String sourcetypeProperty) {
        this.sourcetypeProperty = sourcetypeProperty;
    }

    public boolean hasTimestampProperty() {
        return timestampProperty != null && !timestampProperty.isEmpty();
    }

    public String getTimestampProperty() {
        return timestampProperty;
    }

    public void setTimestampProperty(String timestampProperty) {
        this.timestampProperty = timestampProperty;
    }

    public boolean hasPropertyNameReplacements() {
        return propertyNameReplacements != null && !propertyNameReplacements.isEmpty();
    }

    public Map<String, String> getPropertyNameReplacements() {
        return propertyNameReplacements;
    }

    /**
     * Set the list of property name fragments and their replacements.
     *
     * @param propertyNameReplacements map of JMS Property name fragments and their associated replacements.
     */
    public void setPropertyNameReplacements(Map<String, String> propertyNameReplacements) {
        if (propertyNameReplacements != null && !propertyNameReplacements.isEmpty()) {
            if (this.propertyNameReplacements == null) {
                this.propertyNameReplacements = new HashMap<>();
            } else {
                propertyNameReplacements.clear();
            }
            this.propertyNameReplacements.putAll(propertyNameReplacements);
        }
    }

    /**
     * Set a property name fragment and its replacement.
     *
     * @param propertyNameFragment the property name fragment.
     * @param propertyNameFragmentReplacement the replacement for the property name fragment.
     */
    public void setPropertyNameReplacement(String propertyNameFragment, String propertyNameFragmentReplacement) {
        if (propertyNameFragment != null && !propertyNameFragment.isEmpty()
                && propertyNameFragmentReplacement != null && !propertyNameFragmentReplacement.isEmpty()) {
            if (this.propertyNameReplacements == null) {
                this.propertyNameReplacements = new HashMap<>();
            }
            this.propertyNameReplacements.put(propertyNameFragment, propertyNameFragmentReplacement);
        }
    }

    protected void extractMessageHeadersToMap(Message jmsMessage, Map<String, Object> targetMap) {
        if (jmsMessage != null && targetMap != null) {
            final String logMessageFormat = "Error Reading JMS Message Header '{}' - ignoring";

            try (SplunkMDCHelper helper = createMdcHelper()) {
                if (includeJmsTimestamp) {
                    try {
                        // JMSTimestamp is a little special - we use it for the Event timestamp as well as a field
                        long jmsTimestamp = jmsMessage.getJMSTimestamp();
                        if (jmsTimestamp > 0) {
                            targetMap.put(JMS_TIMESTAMP, String.valueOf(jmsTimestamp));
                        }
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_TIMESTAMP);
                    }
                }

                if (includeJmsDestination) {
                    try {
                        // JMSDestination is a little special - we use it for the source as well as a field
                        Destination jmsDestination = jmsMessage.getJMSDestination();
                        if (jmsDestination != null) {
                            String destinationString = jmsDestination.toString();
                            targetMap.put(JMS_DESTINATION, destinationString);
                        }
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_DESTINATION);
                    }
                }

                if (includeJmsDeliveryMode) {
                    try {
                        int jmsDeliveryMode = jmsMessage.getJMSDeliveryMode();
                        switch (jmsDeliveryMode) {
                            case DeliveryMode.NON_PERSISTENT:
                                targetMap.put(JMS_DELIVERY_MODE, "NON_PERSISTENT");
                                break;
                            case DeliveryMode.PERSISTENT:
                                targetMap.put(JMS_DELIVERY_MODE, "PERSISTENT");
                                break;
                            default:
                                // No-op
                                break;
                        }
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_DELIVERY_MODE);
                    }
                }

                if (includeJmsExpiration) {
                    try {
                        long jmsExpiration = jmsMessage.getJMSExpiration();
                        if (jmsExpiration > 0) {
                            targetMap.put(JMS_EXPIRATION, String.valueOf(jmsExpiration));
                        }
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_EXPIRATION);
                    }
                }

                if (includeJmsPriority) {
                    try {
                        int jmsPriority = jmsMessage.getJMSPriority();
                        if (jmsPriority > 0 && jmsPriority < 10) {
                            targetMap.put(JMS_PRIORITY, String.valueOf(jmsPriority));
                        }
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_PRIORITY);
                    }
                }

                if (includeJmsMessageId) {
                    try {
                        String jmsMessageID = jmsMessage.getJMSMessageID();
                        if (jmsMessageID != null && !jmsMessageID.isEmpty()) {
                            targetMap.put(JMS_MESSAGE_ID, jmsMessageID);
                        }
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_MESSAGE_ID);
                    }
                }

                if (includeJmsCorrelationId) {
                    try {
                        String jmsCorrelationID = jmsMessage.getJMSCorrelationID();
                        if (jmsCorrelationID != null && !jmsCorrelationID.isEmpty()) {
                            targetMap.put(JMS_CORRELATION_ID, jmsCorrelationID);
                        }
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_CORRELATION_ID);
                    }
                }

                if (includeJmsReplyTo) {
                    try {
                        Destination jmsReplyTo = jmsMessage.getJMSReplyTo();
                        if (jmsReplyTo != null) {
                            targetMap.put(JMS_REPLY_TO, jmsReplyTo.toString());
                        }
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_REPLY_TO);
                    }
                }

                if (includeJmsType) {
                    try {
                        String jmsType = jmsMessage.getJMSType();
                        if (jmsType != null && !jmsType.isEmpty()) {
                            targetMap.put(JMS_TYPE, jmsType);
                        } else {
                            targetMap.put(JMS_TYPE, jmsMessage.getClass().getName());
                        }
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_TYPE);
                    }
                }

                if (includeJmsRedelivered) {
                    try {
                        boolean jmsRedelivered = jmsMessage.getJMSRedelivered();
                        targetMap.put(JMS_REDELIVERED, String.valueOf(jmsRedelivered));
                    } catch (JMSException jmsHeaderEx) {
                        log.warn(logMessageFormat, JMS_REDELIVERED);
                    }
                }

            }
        }
    }

    protected void extractMessagePropertiesToMap(Message jmsMessage, Map<String, Object> targetMap) {
        if (jmsMessage != null && targetMap != null) {
            try (SplunkMDCHelper helper = createMdcHelper()) {
                Enumeration<String> propertyNames = jmsMessage.getPropertyNames();
                if (propertyNames != null) {
                    while (propertyNames.hasMoreElements()) {
                        String propertyName = propertyNames.nextElement();
                        try {
                            Object propertyValue = jmsMessage.getObjectProperty(propertyName);
                            if (propertyValue != null) {
                                String propertyStringValue = propertyValue.toString();
                                if (!propertyStringValue.isEmpty()) {
                                    if (hasPropertyNameReplacements()) {
                                        for (Map.Entry<String, String> replacementEntry : propertyNameReplacements.entrySet()) {
                                            propertyName = propertyName.replaceAll(replacementEntry.getKey(), replacementEntry.getValue());
                                        }
                                    }
                                    targetMap.put(propertyName, propertyStringValue);
                                }
                            }
                        } catch (JMSException getObjectPropertyEx) {
                            log.warn("Exception encountered getting property value for property name '{}' - ignoring", propertyName, getObjectPropertyEx);
                        }
                    }
                }
            } catch (JMSException getPropertyNamesEx) {
                log.warn("Exception encountered getting property names - ignoring", getPropertyNamesEx);
            }
        }
    }

    @Override
    public EventBuilder<Message> duplicate() {
        log.trace("Duplicating JmsMessageEventBuilder");

        JmsMessageEventBuilder answer = new JmsMessageEventBuilder();

        answer.copyConfiguration(this);

        return answer;
    }

    @Override
    public String getHostFieldValue() {
        String answer = null;

        if (!hasHost() && hasHostProperty()) {
            answer = getHeaderOrPropertyValue(hostProperty);
        }

        if (answer == null || answer.isEmpty()) {
            answer = super.getHostFieldValue();
        }

        return answer;
    }

    @Override
    public String getIndexFieldValue() {
        String answer = null;

        if (!hasIndex() && hasIndexProperty()) {
            answer = getHeaderOrPropertyValue(indexProperty);
        }

        if (answer == null || answer.isEmpty()) {
            answer = super.getIndexFieldValue();
        }

        return answer;
    }

    @Override
    public String getSourceFieldValue() {
        String answer = null;

        if (!hasSource()) {
            if (hasSourceProperty()) {
                answer = getHeaderOrPropertyValue(sourceProperty);
            } else {
                answer = getHeaderOrPropertyValue("JMSDestination");
            }
        }

        if (answer == null || answer.isEmpty()) {
            answer = super.getSourceFieldValue();
        }

        return answer;
    }

    @Override
    public String getSourcetypeFieldValue() {
        String answer = null;

        if (!hasSourcetype() && hasSourcetypeProperty()) {
            answer = getHeaderOrPropertyValue(sourcetypeProperty);
        }

        if (answer == null || answer.isEmpty()) {
            answer = super.getSourcetypeFieldValue();
        }

        return answer;
    }

    @Override
    public String getTimestampFieldValue() {
        String answer = null;

        if (!hasTimestamp()) {
            if (hasTimestampProperty()) {
                answer = getHeaderOrPropertyValue(timestampProperty);
            } else {
                answer = getHeaderOrPropertyValue("JMSTimestamp");
            }
        }

        if (answer == null || answer.isEmpty()) {
            answer = super.getTimestampFieldValue();
        }

        return answer;
    }

    @Override
    protected void addAdditionalFieldsToMap(Map<String, Object> map) {
        if (hasEventBody()) {
            extractMessageHeadersToMap(getEventBody(), map);
            extractMessagePropertiesToMap(getEventBody(), map);
        }

        super.addAdditionalFieldsToMap(map);
    }

    @Override
    protected void addEventBodyToMap(Map<String, Object> map) {
        try (SplunkMDCHelper helper = createMdcHelper()) {
            if (hasEventBody()) {
                Message jmsMessage = getEventBody();
                if (jmsMessage instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) jmsMessage;
                    try {
                        String textBody = textMessage.getText();
                        if (textBody != null && !textBody.isEmpty()) {
                            map.put(EventCollectorInfo.EVENT_BODY_KEY, textBody);
                        } else {
                            map.put(EventCollectorInfo.EVENT_BODY_KEY, "Empty TextMessage");
                        }
                    } catch (JMSException getTextEx) {
                        log.warn("Exception encountered reading text from TextMessage - skipping JMS Message body", getTextEx);
                        map.put(EventCollectorInfo.EVENT_BODY_KEY, "Exception encountered reading text from TextMessage: " + getTextEx.getMessage());
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
                                String eventBody = new String(bodyBytes);
                                map.put(EventCollectorInfo.EVENT_BODY_KEY, eventBody);
                            } catch (JMSException readBytesEx) {
                                log.warn("Exception encountered reading byte[] from BytesMessage - skipping JMS Message body", readBytesEx);
                                final String exceptionEventBody = "Exception encountered reading byte[] from BytesMessage: " + readBytesEx.getMessage();
                                map.put(EventCollectorInfo.EVENT_BODY_KEY, exceptionEventBody);
                            }
                        } else {
                            map.put(EventCollectorInfo.EVENT_BODY_KEY, "Empty BytesMessage");
                        }
                    } catch (JMSException getBodyLengthEx) {
                        log.warn("Exception encountered getting byte[] length from BytesMessage - skipping JMS Message body", getBodyLengthEx);
                        final String exceptionEventBody =  "Exception encountered getting byte[] length from BytesMessage: " + getBodyLengthEx.getMessage();
                        map.put(EventCollectorInfo.EVENT_BODY_KEY, exceptionEventBody);
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
                                    log.warn("Exception encountered retrieving value for key '{}' from MapMessage - ignoring", key, getStringPropertyEx);
                                }
                            }
                            if (mapMessageBody != null && !mapMessageBody.isEmpty()) {
                                map.put(EventCollectorInfo.EVENT_BODY_KEY, mapMessageBody);
                            } else {
                                map.put(EventCollectorInfo.EVENT_BODY_KEY, "Empty MapMessage");
                            }
                        } else {
                            log.warn("Empty or null Enumeration returned by MapMessage.getMapNames()");
                            map.put(EventCollectorInfo.EVENT_BODY_KEY, "Empty or null Enumeration returned by MapMessage.getMapNames()");
                        }
                    } catch (JMSException getMapNamesEx) {
                        log.warn("Exception encountered retrieving keys from MapMessage - ignoring", getMapNamesEx);
                        map.put(EventCollectorInfo.EVENT_BODY_KEY, "Exception encountered retrieving keys from MapMessage: " + getMapNamesEx.getMessage());
                    }
                } else if (jmsMessage instanceof ObjectMessage) {
                    ObjectMessage objectMessage = (ObjectMessage) jmsMessage;
                    try {
                        Serializable objectBody = objectMessage.getObject();
                        if (objectBody != null) {
                            String objectBodyString = objectBody.toString();
                            if (!objectBodyString.isEmpty()) {
                                map.put(EventCollectorInfo.EVENT_BODY_KEY, objectBodyString);
                            } else {
                                map.put(EventCollectorInfo.EVENT_BODY_KEY, "null Object from ObjectMessage.getObject");
                            }
                        } else {
                            map.put(EventCollectorInfo.EVENT_BODY_KEY, "null ObjectMessage");
                        }
                    } catch (JMSException getObjectEx) {
                        log.warn("Exception encountered reading Object from ObjectMessage - skipping JMS Message body", getObjectEx);
                        map.put(EventCollectorInfo.EVENT_BODY_KEY, "Exception encountered reading Object from ObjectMessage: " + getObjectEx.getMessage());
                    }
                } else if (jmsMessage instanceof StreamMessage) {
                    log.warn("Unsupported JMS Message type: {}", jmsMessage.getClass().getName());
                    map.put(EventCollectorInfo.EVENT_BODY_KEY, "StreamMessage: " + jmsMessage.toString());
                } else {
                    log.warn("Unknown JMS Message type: {}", jmsMessage.getClass().getName());
                    map.put(EventCollectorInfo.EVENT_BODY_KEY, "Unknown JMS Message type: " + jmsMessage.getClass().getName() + "\n" + jmsMessage.toString());
                }
            }
        }
    }

    @Override
    protected void copyConfiguration(EventBuilderSupport<Message> sourceEventBuilder) {
        super.copyConfiguration(sourceEventBuilder);

        if (sourceEventBuilder instanceof JmsMessageEventBuilder) {
            JmsMessageEventBuilder sourceJmsMessageEventBuilder = (JmsMessageEventBuilder) sourceEventBuilder;

            this.includeJmsDestination = sourceJmsMessageEventBuilder.includeJmsDestination;
            this.includeJmsDeliveryMode = sourceJmsMessageEventBuilder.includeJmsDeliveryMode;
            this.includeJmsExpiration = sourceJmsMessageEventBuilder.includeJmsExpiration;
            this.includeJmsPriority = sourceJmsMessageEventBuilder.includeJmsPriority;
            this.includeJmsMessageId = sourceJmsMessageEventBuilder.includeJmsMessageId;
            this.includeJmsTimestamp = sourceJmsMessageEventBuilder.includeJmsTimestamp;
            this.includeJmsCorrelationId = sourceJmsMessageEventBuilder.includeJmsCorrelationId;
            this.includeJmsReplyTo = sourceJmsMessageEventBuilder.includeJmsReplyTo;
            this.includeJmsType = sourceJmsMessageEventBuilder.includeJmsType;
            this.includeJmsRedelivered = sourceJmsMessageEventBuilder.includeJmsRedelivered;

            this.includeJmsProperties = sourceJmsMessageEventBuilder.includeJmsProperties;

            if (sourceJmsMessageEventBuilder.hasPropertyNameReplacements()) {
                this.setPropertyNameReplacements(sourceJmsMessageEventBuilder.propertyNameReplacements);
            }
        }

    }

    /**
     * Get the String value of a JMS Header or property.
     *
     * @param headerOrPropertyName the name of the JMS Header or Property
     *
     * @return the String value of the header or property, or null if the header or property is not contained in the message.
     */
    public String getHeaderOrPropertyValue(String headerOrPropertyName) {
        String answer = null;

        try (SplunkMDCHelper helper = createMdcHelper()) {
            if (hasEventBody() && headerOrPropertyName != null && !headerOrPropertyName.isEmpty()) {
                switch (headerOrPropertyName) {
                    case JMS_DESTINATION:
                        Destination jmsDestination = getEventBody().getJMSDestination();
                        if (jmsDestination != null) {
                            answer = jmsDestination.toString();
                        }
                        break;
                    case JMS_DELIVERY_MODE:
                        int jmsDeliveryMode = getEventBody().getJMSDeliveryMode();
                        switch (jmsDeliveryMode) {
                            case DeliveryMode.NON_PERSISTENT:
                                answer = "NON_PERSISTENT";
                                break;
                            case DeliveryMode.PERSISTENT:
                                answer = "PERSISTENT";
                                break;
                            default:
                                // No-op
                                break;
                        }
                        break;
                    case JMS_EXPIRATION:
                        long jmsExpiration = getEventBody().getJMSExpiration();
                        if (jmsExpiration > 0) {
                            answer = String.valueOf(jmsExpiration);
                        }
                        break;
                    case JMS_PRIORITY:
                        int jmsPriority = getEventBody().getJMSPriority();
                        if (jmsPriority > 0 && jmsPriority < 10) {
                            answer = String.valueOf(jmsPriority);
                        }
                        break;
                    case JMS_MESSAGE_ID:
                        String jmsMessageID = getEventBody().getJMSMessageID();
                        if (jmsMessageID != null && !jmsMessageID.isEmpty()) {
                            answer = jmsMessageID;
                        }
                        break;
                    case JMS_TIMESTAMP:
                        long jmsTimestamp = getEventBody().getJMSTimestamp();
                        if (jmsTimestamp > 0) {
                            answer = String.valueOf(jmsTimestamp);
                        }
                        break;
                    case JMS_CORRELATION_ID:
                        String jmsCorrelationID = getEventBody().getJMSCorrelationID();
                        if (jmsCorrelationID != null && !jmsCorrelationID.isEmpty()) {
                            answer = jmsCorrelationID;
                        }
                        break;
                    case JMS_REPLY_TO:
                        Destination jmsReplyTo = getEventBody().getJMSReplyTo();
                        if (jmsReplyTo != null) {
                            answer = jmsReplyTo.toString();
                        }
                        break;
                    case JMS_TYPE:
                        String jmsType = getEventBody().getJMSType();
                        if (jmsType != null && !jmsType.isEmpty()) {
                            answer = jmsType;
                        } else {
                            answer = getEventBody().getClass().getSimpleName();
                        }
                        break;
                    case JMS_REDELIVERED:
                        boolean jmsRedelivered = getEventBody().getJMSRedelivered();
                        answer = Boolean.toString(jmsRedelivered);
                        break;
                    default:
                        answer = getEventBody().getStringProperty(headerOrPropertyName);
                }
        }
        } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading {}", headerOrPropertyName, jmsEx);
        }

        return answer;
    }

    @Override
    protected void appendConfiguration(StringBuilder builder) {
        super.appendConfiguration(builder);

        builder.append(" includeJmsDestination='").append(includeJmsDestination).append('\'')
                .append(" includeJmsDeliveryMode='").append(includeJmsDeliveryMode).append('\'')
                .append(" includeJmsExpiration='").append(includeJmsExpiration).append('\'')
                .append(" includeJmsPriority='").append(includeJmsPriority).append('\'')
                .append(" includeJmsMessageId='").append(includeJmsMessageId).append('\'')
                .append(" includeJmsTimestamp='").append(includeJmsTimestamp).append('\'')
                .append(" includeJmsCorrelationId='").append(includeJmsCorrelationId).append('\'')
                .append(" includeJmsReplyTo='").append(includeJmsReplyTo).append('\'')
                .append(" includeJmsType='").append(includeJmsType).append('\'')
                .append(" includeJmsRedelivered='").append(includeJmsRedelivered).append('\'')
                .append(" includeJmsProperties='").append(includeJmsProperties).append('\'');

        if (hasPropertyNameReplacements()) {
            builder.append(" propertyNameReplacements='").append(propertyNameReplacements).append('\'');
        }

        return;
    }

}
