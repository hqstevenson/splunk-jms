/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the License.
 */

package com.pronoia.splunk.jms.eventbuilder;

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorInfo;
import com.pronoia.splunk.eventcollector.eventbuilder.EventBuilderSupport;
import com.pronoia.splunk.eventcollector.eventbuilder.JacksonEventBuilderSupport;

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

public class JmsMessageEventBuilder extends JacksonEventBuilderSupport<Message> {
  public static final String JMS_DESTINATION_FIELD = "JMSDestination";
  public static final String JMS_DELIVERY_MODE_FIELD = "JMSDeliveryMode";
  public static final String JMS_EXPIRATION_FIELD = "JMSExpiration";
  public static final String JMS_PRIORITY_FIELD = "JMSPriority";
  public static final String JMS_MESSAGE_ID_FIELD = "JMSMessageID";
  public static final String JMS_TIMESTAMP_FIELD = "JMSTimestamp";
  public static final String JMS_CORRELATION_ID_FIELD = "JMSCorrelationID";
  public static final String JMS_REPLY_TO_FIELD = "JMSReplyTo";
  public static final String JMS_TYPE_FIELD = "JMSType";
  public static final String JMS_REDELIVERED_FIELD = "JMSRedelivered";

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
  String sourceProperty = "JMSDestination";
  String sourcetypeProperty;
  String timestampProperty = "JMSTimestamp";

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

  public void setPropertyNameReplacement(String propertyNameFragment, String propertyNameFragmentReplacement) {
    if (propertyNameFragment != null && !propertyNameFragment.isEmpty() && propertyNameFragmentReplacement != null && !propertyNameFragmentReplacement.isEmpty()) {
      if (this.propertyNameReplacements == null) {
        this.propertyNameReplacements = new HashMap<>();
      }
      this.propertyNameReplacements.put(propertyNameFragment, propertyNameFragmentReplacement);
    }
  }

  protected void extractMessageHeadersToMap(Message jmsMessage, Map<String, Object> targetMap) {
    if (jmsMessage != null  && targetMap != null) {
      final String logMessageFormat = "Error Reading JMS Message Header '{}' - ignoring";

      if (includeJmsTimestamp) {
        try {
          // JMSTimestamp is a little special - we use it for the Event timestamp as well as a field
          long jmsTimestamp = jmsMessage.getJMSTimestamp();
          if (jmsTimestamp > 0) {
            targetMap.put(JMS_TIMESTAMP_FIELD, String.valueOf(jmsTimestamp));
          }
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_TIMESTAMP_FIELD);
        }
      }

      if (includeJmsDestination) {
        try {
          // JMSDestination is a little special - we use it for the source as well as a field
          Destination jmsDestination = jmsMessage.getJMSDestination();
          if (jmsDestination != null) {
            String destinationString = jmsDestination.toString();
            targetMap.put(JMS_DESTINATION_FIELD, destinationString);
          }
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_DESTINATION_FIELD);
        }
      }

      if (includeJmsDeliveryMode) {
        try {
          int jmsDeliveryMode = jmsMessage.getJMSDeliveryMode();
          switch (jmsDeliveryMode) {
            case DeliveryMode.NON_PERSISTENT:
              targetMap.put(JMS_DELIVERY_MODE_FIELD, "NON_PERSISTENT");
              break;
            case DeliveryMode.PERSISTENT:
              targetMap.put(JMS_DELIVERY_MODE_FIELD, "PERSISTENT");
              break;
          }
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_DELIVERY_MODE_FIELD);
        }
      }

      if (includeJmsExpiration) {
        try {
          long jmsExpiration = jmsMessage.getJMSExpiration();
          if (jmsExpiration > 0) {
            targetMap.put(JMS_EXPIRATION_FIELD, String.valueOf(jmsExpiration));
          }
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_EXPIRATION_FIELD);
        }
      }

      if (includeJmsPriority) {
        try {
          int jmsPriority = jmsMessage.getJMSPriority();
          if (jmsPriority > 0 && jmsPriority < 10) {
            targetMap.put(JMS_PRIORITY_FIELD, String.valueOf(jmsPriority));
          }
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_PRIORITY_FIELD);
        }
      }

      if (includeJmsMessageId) {
        try {
          String jmsMessageID = jmsMessage.getJMSMessageID();
          if (jmsMessageID != null && !jmsMessageID.isEmpty()) {
            targetMap.put(JMS_MESSAGE_ID_FIELD, jmsMessageID);
          }
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_MESSAGE_ID_FIELD);
        }
      }

      if (includeJmsCorrelationId) {
        try {
          String jmsCorrelationID = jmsMessage.getJMSCorrelationID();
          if (jmsCorrelationID != null && !jmsCorrelationID.isEmpty()) {
            targetMap.put(JMS_CORRELATION_ID_FIELD, jmsCorrelationID);
          }
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_CORRELATION_ID_FIELD);
        }
      }

      if (includeJmsReplyTo) {
        try {
          Destination jmsReplyTo = jmsMessage.getJMSReplyTo();
          if (jmsReplyTo != null) {
            targetMap.put(JMS_REPLY_TO_FIELD, jmsReplyTo.toString());
          }
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_REPLY_TO_FIELD);
        }
      }

      if (includeJmsType) {
        try {
          String jmsType = jmsMessage.getJMSType();
          if (jmsType != null && !jmsType.isEmpty()) {
            targetMap.put(JMS_TYPE_FIELD, jmsType);
          } else {
            targetMap.put(JMS_TYPE_FIELD, jmsMessage.getClass().getName());
          }
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_TYPE_FIELD);
        }
      }

      if (includeJmsRedelivered) {
        try {
          boolean jmsRedelivered = jmsMessage.getJMSRedelivered();
          targetMap.put(JMS_REDELIVERED_FIELD, String.valueOf(jmsRedelivered));
        } catch (JMSException jmsHeaderEx) {
          log.warn(logMessageFormat, JMS_REDELIVERED_FIELD);
        }
      }

    }
  }

  protected void extractMessagePropertiesToMap(Message jmsMessage, Map<String, Object> targetMap) {
    if (jmsMessage != null && targetMap != null) {
      try {
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
              String logMessage = String.format("Exception encountered getting property value for property name '{}' - ignoring", propertyName);
              log.warn(logMessage, getObjectPropertyEx);
            }
          }
        }
      } catch (JMSException getPropertyNamesEx) {
        String logMessage = String.format("Exception encountered getting property names - ignoring");
        log.warn(logMessage, getPropertyNamesEx);
      }
    }
  }

  @Override
  public EventBuilder<Message> duplicate() {
    JmsMessageEventBuilder answer = new JmsMessageEventBuilder();

    answer.copyConfiguration(this);

    return answer;
  }

  @Override
  public String getHostFieldValue() {
    String answer = null;

    if (hasHostProperty()) {
      String value = getHeaderOrPropertyValue(indexProperty);
      if (value != null && !value.isEmpty()) {
        answer = value;
      }
    } else {
      answer = super.getHostFieldValue();
    }

    return answer;
  }

  @Override
  public String getIndexFieldValue() {
    String answer = null;

    if (hasIndexProperty()) {
      String value = getHeaderOrPropertyValue(indexProperty);
      if (value != null && !value.isEmpty()) {
        answer = value;
      }
    } else {
      answer = super.getIndexFieldValue();
    }

    return answer;
  }

  @Override
  public String getSourceFieldValue() {
    String answer = null;

    if (hasSourceProperty()) {
      String value = getHeaderOrPropertyValue(sourceProperty);
      if (value != null && !value.isEmpty()) {
        answer = value;
      }
    } else {
      answer = super.getSourceFieldValue();
    }

    return answer;
  }

  @Override
  public String getSourcetypeFieldValue() {
    String answer = null;

    if (hasSourcetypeProperty()) {
      String value = getHeaderOrPropertyValue(sourcetypeProperty);
      if (value != null && !value.isEmpty()) {
        answer = value;
      }
    } else {
      answer = super.getSourcetypeFieldValue();
    }

    return answer;
  }

  @Override
  public String getTimestampFieldValue() {
    String answer = null;

    if (hasTimestampProperty()) {
      String value = getHeaderOrPropertyValue(timestampProperty);
      if (value != null && !value.isEmpty()) {
        answer = value;
      }
    } else {
      answer = super.getTimestampFieldValue();
    }

    return answer;
  }

  @Override
  protected void addAdditionalFieldsToMap(Map<String, Object> map) {
    if (hasEventBody()) {
      extractMessageHeadersToMap(getEventBody(), getFields());
      extractMessagePropertiesToMap(getEventBody(), getFields());
    }

    super.addAdditionalFieldsToMap(map);
  }

  @Override
  protected void addEventBodyToMap(Map<String, Object> map) {
    if (hasEventBody()) {
      Message jmsMessage = getEventBody();
      if (jmsMessage instanceof TextMessage) {
        TextMessage textMessage = (TextMessage) jmsMessage;
        try {
          String textBody = textMessage.getText();
          if (textBody != null) {
            map.put(EventCollectorInfo.EVENT_BODY_KEY, textBody);
          }
        } catch (JMSException getTextEx) {
          log.warn("Exception encountered reading text from TextMessage - skipping JMS Message body", getTextEx);
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
            }
          }
        } catch (JMSException getBodyLengthEx) {
          log.warn("Exception encountered getting byte[] length from BytesMessage - skipping JMS Message body", getBodyLengthEx);
        }
      } else if (jmsMessage instanceof MapMessage) {
        MapMessage mapMessage = (MapMessage) jmsMessage;
        if (mapMessage != null) {
          try {
            Enumeration<String> keys = mapMessage.getMapNames();
            if (keys != null) {
              Map<String, Object> mapMessageBody = new LinkedHashMap<>();
              while (keys.hasMoreElements()) {
                String key = keys.nextElement();
                try {
                  String value = mapMessage.getString(key);
                  if (value != null) {
                    mapMessageBody.put(key, value);
                  }
                } catch (JMSException getStringPropertyEx) {
                  String logMessage = String.format("Exception encountered retrieving value for key '%s' from MapMessage - ignoring", key);
                  log.warn(logMessage, getStringPropertyEx);
                }
              }
              map.put(EventCollectorInfo.EVENT_BODY_KEY, mapMessageBody);
            }
          } catch (JMSException getMapNamesEx) {
            log.warn("Exception encountered retrieving keys from MapMessage - ignoring", getMapNamesEx);
          }
        }
      } else if (jmsMessage instanceof ObjectMessage) {
        ObjectMessage objectMessage = (ObjectMessage) jmsMessage;
        try {
          Serializable objectBody = objectMessage.getObject();
          if (objectBody != null) {
            map.put(EventCollectorInfo.EVENT_BODY_KEY, objectBody.toString());
          }
        } catch (JMSException getObjectEx) {
          log.warn("Exception encountered reading Object from ObjectMessage - skipping JMS Message body", getObjectEx);
        }
      } else if (jmsMessage instanceof StreamMessage) {
        log.warn("Unsupported JMS Message type: {}", jmsMessage.getClass().getName());
        map.put(EventCollectorInfo.EVENT_BODY_KEY, jmsMessage.toString());
      } else {
        log.warn("Unknown JMS Message type: {}", jmsMessage.getClass().getName());
        map.put(EventCollectorInfo.EVENT_BODY_KEY, jmsMessage.toString());
      }
    }
  }

  @Override
  protected void copyConfiguration(EventBuilderSupport<Message> sourceEventBuilder) {
    super.copyConfiguration(sourceEventBuilder);

    if (sourceEventBuilder instanceof JmsMessageEventBuilder) {
      JmsMessageEventBuilder sourceJmsMessageEventBuilder = (JmsMessageEventBuilder) sourceEventBuilder;
      if (sourceJmsMessageEventBuilder.hasPropertyNameReplacements()) {
        this.setPropertyNameReplacements(sourceJmsMessageEventBuilder.propertyNameReplacements);
      }
    }

  }

  public String getHeaderOrPropertyValue(String headerOrPropertyName) {
    String answer = null;

    if (hasEventBody() && headerOrPropertyName != null && !headerOrPropertyName.isEmpty()) {
      switch (headerOrPropertyName) {
        case "JMSDestination":
          try {
            Destination jmsDestination = getEventBody().getJMSDestination();
            if (jmsDestination != null) {
              answer = jmsDestination.toString();
            }
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSDestination header", jmsEx);
          }
          break;
        case "JMSDeliveryMode":
          try {
            int jmsDeliveryMode = getEventBody().getJMSDeliveryMode();
            switch (jmsDeliveryMode) {
              case DeliveryMode.NON_PERSISTENT:
                answer = "NON_PERSISTENT";
                break;
              case DeliveryMode.PERSISTENT:
                answer = "PERSISTENT";
                break;
            }
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSDeliveryMode header", jmsEx);
          }
          break;
        case "JMSExpiration":
          try {
            long jmsExpiration = getEventBody().getJMSExpiration();
            if (jmsExpiration > 0) {
              answer = String.valueOf(jmsExpiration);
            }
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSExpiration header", jmsEx);
          }
          break;
        case "JMSPriority":
          try {
            int jmsPriority = getEventBody().getJMSPriority();
            if (jmsPriority > 0 && jmsPriority < 10) {
              answer = String.valueOf(jmsPriority);
            }
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSPriority header", jmsEx);
          }
          break;
        case "JMSMessageID":
          try {
            String jmsMessageID = getEventBody().getJMSMessageID();
            if (jmsMessageID != null && !jmsMessageID.isEmpty()) {
              answer = jmsMessageID;
            }
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSMessageID header", jmsEx);
          }
          break;
        case "JMSTimestamp":
          try {
            long jmsTimestamp = getEventBody().getJMSTimestamp();
            if (jmsTimestamp > 0) {
              answer = String.valueOf(jmsTimestamp);
            }
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSTimestamp header", jmsEx);
          }
          break;
        case "JMSCorrelationID":
          try {
            String jmsCorrelationID = getEventBody().getJMSCorrelationID();
            if (jmsCorrelationID != null && !jmsCorrelationID.isEmpty()) {
              answer = jmsCorrelationID;
            }
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSCorrelationID header", jmsEx);
          }
          break;
        case "JMSReplyTo":
          try {
            Destination jmsReplyTo = getEventBody().getJMSReplyTo();
            if (jmsReplyTo != null) {
              answer = jmsReplyTo.toString();
            }
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSReplyTo header", jmsEx);
          }
          break;
        case "JMSType":
          try {
            String jmsType = getEventBody().getJMSType();
            if (jmsType != null && !jmsType.isEmpty()) {
              answer = jmsType;
            }
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSType header", jmsEx);
          }
          break;
        case "JMSRedelivered":
          try {
            boolean jmsRedelivered = getEventBody().getJMSRedelivered();
            answer = Boolean.toString(jmsRedelivered);
          } catch (JMSException jmsEx) {
            log.warn("Exception encountered reading JMSRedelivered header", jmsEx);
          }
          break;
        default:
      }
    }

    return answer;
  }

}
