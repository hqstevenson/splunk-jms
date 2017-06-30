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

package com.pronoia.splunk.jms.activemq;

import com.pronoia.splunk.eventcollector.EventCollectorClient;
import com.pronoia.splunk.jms.SplunkJmsMessageListener;
import com.pronoia.splunk.jms.builder.JmsMessageEventBuilder;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.relation.MBeanServerNotificationFilter;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives JMS Messages from an ActiveMQ broker in the same JVM and delivers them to Splunk using the HTTP Event
 * Collector.
 *
 * <p>If the broker name isn't configured, the broker is located using JMX - the first broker returned by the JMX
 * queury will be used.
 */
public class SplunkEmbeddedActiveMQMessageListenerFactory implements NotificationListener {
  static final String ACTIVEMQ_QUEUE_OBJECT_NAME_PATTERN = "org.apache.activemq:type=Broker,brokerName=%s,destinationType=Queue,destinationName=%s";

  Logger log = LoggerFactory.getLogger(this.getClass());

  String brokerName = "*";
  String userName;
  String password;

  String destinationName = "audit.*";

  String splunkIndex;
  String splunkSource;
  String splunkSourcetype;

  EventCollectorClient splunkClient;

  public boolean hasBrokerName() {
    return brokerName != null && !brokerName.isEmpty();
  }

  public String getBrokerName() {
    return brokerName;
  }

  public void setBrokerName(String brokerName) {
    this.brokerName = brokerName;
  }

  public boolean hasUserName() {
    return userName != null && !userName.isEmpty();
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public boolean hasPassword() {
    return password != null && !password.isEmpty();
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public boolean hasQueueName() {
    return destinationName != null && !destinationName.isEmpty();
  }

  public String getDestinationName() {
    return destinationName;
  }

  public void setDestinationName(String destinationName) {
    this.destinationName = destinationName;
  }

  public boolean hasSplunkIndex() {
    return splunkIndex != null && !splunkIndex.isEmpty();
  }

  public String getSplunkIndex() {
    return splunkIndex;
  }

  public void setSplunkIndex(String splunkIndex) {
    this.splunkIndex = splunkIndex;
  }

  public boolean hasSplunkSource() {
    return splunkSource != null && !splunkSource.isEmpty();
  }

  public String getSplunkSource() {
    return splunkSource;
  }

  public void setSplunkSource(String splunkSource) {
    this.splunkSource = splunkSource;
  }

  public boolean hasSplunkSourceType() {
    return splunkSourcetype != null && !splunkSourcetype.isEmpty();
  }

  public String getSplunkSourcetype() {
    return splunkSourcetype;
  }

  public void setSplunkSourcetype(String splunkSourcetype) {
    this.splunkSourcetype = splunkSourcetype;
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

  void verifyConfiguration() throws IllegalStateException {
    if (!hasQueueName()) {
      throw new IllegalStateException("Queue Name is required");
    }

    if (!hasSplunkClient()) {
      throw new IllegalStateException("Splunk Client must be specified");
    }

    if (!hasBrokerName()) {
      brokerName = "*";
    }
  }

   /**
   * Start NotificationListener, watching for the specific queue.
   */
  public void start() {
    ObjectName objectNameFilter;

    String objectNameString = String.format(ACTIVEMQ_QUEUE_OBJECT_NAME_PATTERN,
       hasBrokerName() ? brokerName : '*', destinationName
    );

    try {
      objectNameFilter = new ObjectName(objectNameString);
    } catch (MalformedObjectNameException malformedObjectNameEx) {
      // TODO: Rework this error message
      String errorMessage = String.format("Default ObjectName '%s' is malformed", objectNameString);
      throw new IllegalArgumentException(errorMessage, malformedObjectNameEx);
    }

    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    MBeanServerNotificationFilter notificationFilter = new MBeanServerNotificationFilter();
    notificationFilter.enableObjectName(objectNameFilter);
    notificationFilter.enableType(MBeanServerNotification.REGISTRATION_NOTIFICATION);

    try {
      mbeanServer.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this, notificationFilter, null);
    } catch (InstanceNotFoundException e) {
      // TODO:  Handle this
      e.printStackTrace();
    }

    // Now look for anything that was created before the Notification Listener was started

    Set<ObjectName> objectNameSet = mbeanServer.queryNames(objectNameFilter, null);
    if (objectNameSet != null && !objectNameSet.isEmpty()) {
      for (ObjectName objectName : objectNameSet) {
        SplunkJmsMessageListener queueListener = createMessageListener(objectName);

        queueListener.start();
      }
    }
  }

  /**
   * Stop the NotificationListener.
   */
  public void stop() {
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      mbeanServer.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this);
    } catch (InstanceNotFoundException e) {
      // Should never get this one - the MBeanServerDelegate.DELEGATE_NAME should always be there
      e.printStackTrace();
    } catch (ListenerNotFoundException e) {
      // TODO: Handle this
      // Log something - ignore this error
      e.printStackTrace();
    }

  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    if (notification instanceof MBeanServerNotification) {
      MBeanServerNotification serverNotification = (MBeanServerNotification) notification;
      if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(notification.getType())) {
        ObjectName mbeanName = serverNotification.getMBeanName();
        log.info("Detected MBean Registration '{}'", mbeanName.getCanonicalName());
        SplunkJmsMessageListener queueListener = createMessageListener(mbeanName);

        queueListener.start();
      }
    }
  }

  SplunkJmsMessageListener createMessageListener(ObjectName objectName) {
    log.info("Creating Message listener for '{}'", objectName);

    SplunkJmsMessageListener queueListener = new SplunkJmsMessageListener();

    ActiveMQConnectionFactory tmpConnectionFactory = new ActiveMQConnectionFactory();
    tmpConnectionFactory.setBrokerURL(String.format("vm://%s?create=false&waitForStart=60000", objectName.getKeyProperty("brokerName")));
    if (hasUserName()) {
      tmpConnectionFactory.setUserName(userName);
    }
    if (hasPassword()) {
      tmpConnectionFactory.setPassword(password);
    }

    queueListener.setConnectionFactory(tmpConnectionFactory);

    JmsMessageEventBuilder builder = new JmsMessageEventBuilder();
    builder.setHost();
    if (hasSplunkIndex()) {
      builder.setIndex(splunkIndex);
    }
    if (hasSplunkSource()) {
      builder.setSource(splunkSource);
    }
    if (hasSplunkSourceType()) {
      builder.setSourcetype(splunkSourcetype);
    }

    queueListener.setMessageEventBuilder(builder);
    queueListener.setSplunkClient(splunkClient);

    queueListener.setDestinationName(objectName.getKeyProperty("destinationName"));

    return queueListener;
  }
}
