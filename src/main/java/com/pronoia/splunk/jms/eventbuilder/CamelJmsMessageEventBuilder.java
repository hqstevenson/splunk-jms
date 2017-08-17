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

  @Override
  public EventBuilder<Message> duplicate() {
    CamelJmsMessageEventBuilder answer = new CamelJmsMessageEventBuilder();

    answer.copyConfiguration(this);

    return answer;
  }

}
