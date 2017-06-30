package com.pronoia.splunk.jms.builder;

import com.pronoia.splunk.eventcollector.EventCollectorInfo;
import com.pronoia.splunk.eventcollector.builder.EventBuilderSupport;

import java.io.Serializable;
import java.util.Enumeration;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.json.simple.JSONObject;

public class JmsMessageEventBuilder extends EventBuilderSupport<Message> {
  final String containerName = System.getProperty("karaf.name");

  public boolean hasContainerName() {
    return containerName != null && !containerName.isEmpty();
  }

  public String getContainerName() {
    return containerName;
  }

  @Override
  protected void serializeFields(JSONObject eventObject) {
    if (hasContainerName()) {
      addField(JmsMessageEventConstants.CONTAINER_FIELD, containerName);
    }
    if (hasEvent()) {
      extractMessageHeaderFields();
      extractMessagePropertyFields();
    }

    super.serializeFields(eventObject);
  }

  @Override
  protected void serializeBody(JSONObject eventObject) {
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

  void extractMessageHeaderFields() {
    if (hasEvent()) {
      final String logMessageFormat = "Error Reading JMS Message Header '{}' - ignoring";

      Message jmsMessage = getEvent();

      try {
        // JMSTimestamp is a little special - we use it for the Event timestamp as well as a field
        Long jmsTimestamp = jmsMessage.getJMSTimestamp();
        if (jmsTimestamp != null) {
          addField(JmsMessageEventConstants.JMS_TIMESTAMP_FIELD, jmsTimestamp.toString());
          setTimestamp( jmsTimestamp / 1000.0);
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_TIMESTAMP_FIELD);
      }

      try {
        // JMSDestination is a little special - we use it for the source as well as a field
        Destination jmsDestination = jmsMessage.getJMSDestination();
        if (jmsDestination != null) {
          String destinationString = jmsDestination.toString();
          addField(JmsMessageEventConstants.JMS_DESTINATION_FIELD, destinationString);
          if (!hasSource()) {
            setSource(destinationString);
          }
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_DESTINATION_FIELD);
      }

      try {
        Integer jmsDeliveryMode = jmsMessage.getJMSDeliveryMode();
        if (jmsDeliveryMode != null) {
          addField(JmsMessageEventConstants.JMS_DELIVERY_MODE_FIELD, jmsDeliveryMode.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_DELIVERY_MODE_FIELD);
      }

      try {
        Long jmsExpiration = jmsMessage.getJMSExpiration();
        if (jmsExpiration != null && jmsExpiration != 0) {
          addField(JmsMessageEventConstants.JMS_EXPIRATION_FIELD, jmsExpiration.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_EXPIRATION_FIELD);
      }

      try {
        Integer jmsPriority = jmsMessage.getJMSPriority();
        if (jmsPriority != null) {
          addField(JmsMessageEventConstants.JMS_PRIORITY_FIELD, jmsPriority.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_PRIORITY_FIELD);
      }

      try {
        String jmsMessageID = jmsMessage.getJMSMessageID();
        if (jmsMessageID != null && !jmsMessageID.isEmpty()) {
          addField(JmsMessageEventConstants.JMS_MESSAGE_ID_FIELD, jmsMessageID);
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_MESSAGE_ID_FIELD);
      }

      try {
        String jmsCorrelationID = jmsMessage.getJMSCorrelationID();
        if (jmsCorrelationID != null && !jmsCorrelationID.isEmpty()) {
          addField(JmsMessageEventConstants.JMS_CORRELATION_ID_FIELD, jmsCorrelationID);
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_CORRELATION_ID_FIELD);
      }

      try {
        Destination jmsReplyTo = jmsMessage.getJMSReplyTo();
        if (jmsReplyTo != null) {
          addField(JmsMessageEventConstants.JMS_REPLY_TO_FIELD, jmsReplyTo.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_REPLY_TO_FIELD);
      }

      try {
        String jmsType = jmsMessage.getJMSType();
        if (jmsType != null && !jmsType.isEmpty()) {
          addField(JmsMessageEventConstants.JMS_TYPE_FIELD, jmsType);
        } else {
          addField(JmsMessageEventConstants.JMS_TYPE_FIELD, jmsMessage.getClass().getName());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_TYPE_FIELD);
      }

      try {
        Boolean jmsRedelivered = jmsMessage.getJMSRedelivered();
        if (jmsRedelivered != null) {
          addField(JmsMessageEventConstants.JMS_REDELIVERED_FIELD, jmsRedelivered.toString());
        }
      } catch (JMSException jmsHeaderEx) {
        log.warn(logMessageFormat, JmsMessageEventConstants.JMS_REDELIVERED_FIELD);
      }

    } else {
      setTimestamp();
    }
  }

  void extractMessagePropertyFields() {
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

}
