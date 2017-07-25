package com.pronoia.splunk.jms.builder;

import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.Message;

public class CamelJmsMessageEventBuilder extends JmsMessageEventBuilder {
  static final String DOT_STRING = "_DOT_";
  static final String HYPHEN_STRING = "_HYPHEN_";

  @Override
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
                  propertyName = propertyName.replaceAll(DOT_STRING, ".").replaceAll(HYPHEN_STRING, "-");

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
