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

package com.pronoia.splunk.jms.eventbuilder;

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorInfo;
import com.pronoia.splunk.eventcollector.eventbuilder.JacksonEventBuilderSupport;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.json.simple.JSONObject;

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

  @Override
  protected void serializeFields(Map eventObject) {
    if (!hasHost()) {
      setHost();
    }

    if (hasEvent()) {
      extractMessageHeaderFields();
      extractMessagePropertyFields();
    }

    super.serializeFields(eventObject);
  }

  @Override
  protected void serializeBody(Map<String, Object> eventObject) {
    if (hasEvent()) {
      Message jmsMessage = getEvent();
      if (jmsMessage instanceof TextMessage) {
        TextMessage textMessage = (TextMessage) jmsMessage;
        try {
          String textBody = textMessage.getText();
          if (textBody != null) {
            eventObject.put(EventCollectorInfo.EVENT_BODY_KEY, textBody);
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
              eventObject.put(EventCollectorInfo.EVENT_BODY_KEY, eventBody);
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
              JSONObject eventBody = new JSONObject();
              while (keys.hasMoreElements()) {
                String key = keys.nextElement();
                try {
                  String value = mapMessage.getString(key);
                  if (value != null) {
                    eventBody.put(key, value);
                  }
                } catch (JMSException getStringPropertyEx) {
                  String logMessage = String.format("Exception encountered retrieving value for key '%s' from MapMessage - ignoring", key);
                  log.warn(logMessage, getStringPropertyEx);
                }
              }
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
            eventObject.put(EventCollectorInfo.EVENT_BODY_KEY, objectBody.toString());
          }
        } catch (JMSException getObjectEx) {
          log.warn("Exception encountered reading Object from ObjectMessage - skipping JMS Message body", getObjectEx);
        }
      } else if (jmsMessage instanceof StreamMessage) {
        log.warn("Unsupported JMS Message type: {}", jmsMessage.getClass().getName());
        super.serializeBody(eventObject);
      } else {
        log.warn("Unknown JMS Message type: {}", jmsMessage.getClass().getName());
        super.serializeBody(eventObject);
      }
    }
  }

  protected void extractMessageHeaderFields() {
    if (hasEvent()) {
      final String logMessageFormat = "Error Reading JMS Message Header '{}' - ignoring";

      Message jmsMessage = getEvent();

      try {
        // JMSTimestamp is a little special - we use it for the Event timestamp as well as a field
        Long jmsTimestamp = jmsMessage.getJMSTimestamp();
        if (jmsTimestamp != null) {
          addField(JMS_TIMESTAMP_FIELD, jmsTimestamp.toString());
          setTimestamp( jmsTimestamp / 1000.0);
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_TIMESTAMP_FIELD);
      }

      try {
        // JMSDestination is a little special - we use it for the source as well as a field
        Destination jmsDestination = jmsMessage.getJMSDestination();
        if (jmsDestination != null) {
          String destinationString = jmsDestination.toString();
          addField(JMS_DESTINATION_FIELD, destinationString);
          if (!hasSource()) {
            setSource(destinationString);
          }
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_DESTINATION_FIELD);
      }

      try {
        Integer jmsDeliveryMode = jmsMessage.getJMSDeliveryMode();
        if (jmsDeliveryMode != null) {
          addField(JMS_DELIVERY_MODE_FIELD, jmsDeliveryMode.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_DELIVERY_MODE_FIELD);
      }

      try {
        Long jmsExpiration = jmsMessage.getJMSExpiration();
        if (jmsExpiration != null && jmsExpiration != 0) {
          addField(JMS_EXPIRATION_FIELD, jmsExpiration.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_EXPIRATION_FIELD);
      }

      try {
        Integer jmsPriority = jmsMessage.getJMSPriority();
        if (jmsPriority != null) {
          addField(JMS_PRIORITY_FIELD, jmsPriority.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_PRIORITY_FIELD);
      }

      try {
        String jmsMessageID = jmsMessage.getJMSMessageID();
        if (jmsMessageID != null && !jmsMessageID.isEmpty()) {
          addField(JMS_MESSAGE_ID_FIELD, jmsMessageID);
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_MESSAGE_ID_FIELD);
      }

      try {
        String jmsCorrelationID = jmsMessage.getJMSCorrelationID();
        if (jmsCorrelationID != null && !jmsCorrelationID.isEmpty()) {
          addField(JMS_CORRELATION_ID_FIELD, jmsCorrelationID);
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_CORRELATION_ID_FIELD);
      }

      try {
        Destination jmsReplyTo = jmsMessage.getJMSReplyTo();
        if (jmsReplyTo != null) {
          addField(JMS_REPLY_TO_FIELD, jmsReplyTo.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_REPLY_TO_FIELD);
      }

      try {
        String jmsType = jmsMessage.getJMSType();
        if (jmsType != null && !jmsType.isEmpty()) {
          addField(JMS_TYPE_FIELD, jmsType);
        } else {
          addField(JMS_TYPE_FIELD, jmsMessage.getClass().getName());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_TYPE_FIELD);
      }

      try {
        Boolean jmsRedelivered = jmsMessage.getJMSRedelivered();
        if (jmsRedelivered != null) {
          addField(JMS_REDELIVERED_FIELD, jmsRedelivered.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JMS_REDELIVERED_FIELD);
      }

    } else {
      setTimestamp();
    }
  }

  protected void extractMessagePropertyFields() {
    if (hasEvent()) {
      Message jmsMessage = getEvent();
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
                  this.addField(propertyName, propertyStringValue);
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
}
