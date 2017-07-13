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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.activemq.RedeliveryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers ActiveMQ Brokers and queues running in the same JVM (embedded), and creates SplunkJmsMessageLister instances for the discovered queues.
 *
 * By default, SplunkJmsMessageListeners will be setup to consume messages from JMS Queues with names starting with 'audit.' from all detected brokers.
 *
 * Brokers/queues are detected via JMX Notifications from the MBeanServerDelegate.
 */
public class SplunkEmbeddedActiveMQMessageListenerFactory implements NotificationListener {
  static final String DEFAULT_BROKER_NAME = "*";
  static final String DEFAULT_DESTINATION_TYPE = "Queue";
  static final String DEFAULT_DESTINATION_NAME = "audit.*";

  Logger log = LoggerFactory.getLogger(this.getClass());

  String brokerName = DEFAULT_BROKER_NAME;
  String userName;
  String password;

  String destinationType = DEFAULT_DESTINATION_TYPE;
  String destinationName = DEFAULT_DESTINATION_NAME;

  String splunkIndex;
  String splunkSource;
  String splunkSourcetype;

  EventCollectorClient splunkClient;

  Map<String, SplunkJmsMessageListener> listenerMap;

  static ObjectName newObjectName(String string) {
    try {
      return new ObjectName(string);
    } catch (MalformedObjectNameException e) {
      throw new IllegalArgumentException(e);
    }
  }

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

  public boolean hasDestinationType() {
    return destinationType != null && !destinationType.isEmpty();
  }

  public String getDestinationType() {
    return destinationType;
  }

  public void setDestinationType(String destinationType) {
    this.destinationType = destinationType;
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

  public boolean hasSplunkSourcetype() {
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
    if (!hasSplunkClient()) {
      throw new IllegalStateException("Splunk Client must be specified");
    }

    if (!hasBrokerName()) {
      brokerName = DEFAULT_BROKER_NAME;
      log.warn("ActiveMQ Broker Name is not specified - using default '{}'", brokerName);
    }

    if (!hasDestinationType()) {
      destinationType = DEFAULT_DESTINATION_TYPE;
      log.warn("Destination Type is not specified - using default '{}'", destinationType);
    }

    if (!hasDestinationName()) {
      destinationName = DEFAULT_DESTINATION_NAME;
      log.warn("Destination Name is not specified - using default '{}'", destinationName);
    }

  }

  /**
   * Start NotificationListener, watching for the specific queue.
   */
  synchronized public void start() {
    verifyConfiguration();

    final ObjectName destinationObjectNamePattern = createDestinationObjectName();

    log.info("Starting {} with ObjectName {}", this.getClass().getSimpleName(), destinationObjectNamePattern);

    if (listenerMap != null) {
      if (!listenerMap.isEmpty()) {
        log.info("Stopping current MessageListeners in preparation for startup {}", listenerMap.keySet());
        for (SplunkJmsMessageListener messageListener : listenerMap.values()) {
          if (messageListener != null && messageListener.isRunning()) {
            messageListener.stop();
          }
        }
        listenerMap.clear();
      }
    } else {
      listenerMap = new ConcurrentHashMap();
    }

    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    MBeanServerNotificationFilter notificationFilter = new MBeanServerNotificationFilter();
    notificationFilter.enableAllObjectNames();
    notificationFilter.enableType(MBeanServerNotification.REGISTRATION_NOTIFICATION);

    Set<ObjectName> existingDestinationSet = null;

    log.debug("Adding JMX NotificationListener");
    try {
      mbeanServer.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this, notificationFilter, null);
    } catch (InstanceNotFoundException instanceNotFoundEx) {
      // TODO: Handle this
      String errorMessage = String.format("Failed to add NotificationListener to '%s' with filter '%s' for '%s' - dynamic destination detection is disabled",
          MBeanServerDelegate.DELEGATE_NAME.getCanonicalName(),
          notificationFilter.toString(),
          destinationObjectNamePattern.getCanonicalName()
      );
      log.error(errorMessage, instanceNotFoundEx);
    }
  }

  /**
   * Stop the NotificationListener.
   */
  synchronized public void stop() {
    log.info("Stopping {} with ObjectName {}", this.getClass().getSimpleName(), createDestinationObjectName());

    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      mbeanServer.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this);
    } catch (InstanceNotFoundException instanceNotFoundEx) {
      // Should never get this one - the MBeanServerDelegate.DELEGATE_NAME should always be there
      // TODO: Handle this
      String errorMessage = String.format("Failed to removed NotificationListener from '%s'", MBeanServerDelegate.DELEGATE_NAME.toString());
      log.error(errorMessage, instanceNotFoundEx);
    } catch (ListenerNotFoundException listenerNotFoundEx) {
      // TODO: Handle this
      String errorMessage = String.format("Failed to removed NotificationListener from '%s'", MBeanServerDelegate.DELEGATE_NAME.toString());
      log.error(errorMessage, listenerNotFoundEx);
    }

    if (listenerMap != null && !listenerMap.isEmpty()) {
      for (Map.Entry<String, SplunkJmsMessageListener> listenerEntry : listenerMap.entrySet()) {
        SplunkJmsMessageListener messageListener = listenerEntry.getValue();
        if (messageListener != null && messageListener.isRunning()) {
          log.info("Stopping listener for {}", listenerEntry.getKey());
          messageListener.stop();
        }
      }
      listenerMap.clear();
    }
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    if (notification instanceof MBeanServerNotification) {
      MBeanServerNotification serverNotification = (MBeanServerNotification) notification;
      if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(notification.getType())) {
        final ObjectName destinationObjectNamePattern = createDestinationObjectName();

        ObjectName mbeanName = serverNotification.getMBeanName();
        if (destinationObjectNamePattern.apply(mbeanName)) {
          log.info("ObjectName '{}' matched '{}' - creating startup thread.", mbeanName.getCanonicalName(), destinationObjectNamePattern.getCanonicalName());
          new ListenerStartupThread(mbeanName).start();
        } else {
          log.debug("ObjectName '{}' did not match '{}' - ignoring.", mbeanName.getCanonicalName(), destinationObjectNamePattern.getCanonicalName());
        }
      }
    }
  }

  ObjectName createDestinationObjectName() {
    ObjectName destinationObjectName;

    String objectNameString = String.format("org.apache.activemq:type=Broker,brokerName=%s,destinationType=%s,destinationName=%s",
        hasBrokerName() ? brokerName : '*',
        hasDestinationType() ? destinationType : '*',
        hasDestinationName() ? destinationName : '*'
    );

    try {
      destinationObjectName = new ObjectName(objectNameString);
    } catch (MalformedObjectNameException malformedObjectNameEx) {
      String errorMessage = String.format("ObjectName '%s' is malformed", objectNameString);
      throw new IllegalArgumentException(errorMessage, malformedObjectNameEx);
    }

    return destinationObjectName;
  }

  synchronized SplunkJmsMessageListener createMessageListener(ObjectName objectName) {
    String objectNameString = objectName.getCanonicalName();

    SplunkJmsMessageListener queueListener = null;

    if (listenerMap.containsKey(objectNameString)) {
      log.info("Skipping creation of MessageListener for {} - instance already exists", objectName);
      // queueListener = listenerMap.get(objectNameString);
    } else {
      log.info("Creating MessageListener for '{}'", objectName);

      queueListener = new SplunkJmsMessageListener();

      ActiveMQConnectionFactory tmpConnectionFactory = new ActiveMQConnectionFactory();
      // tmpConnectionFactory.setBrokerURL(String.format("vm://%s?create=false&waitForStart=60000", objectName.getKeyProperty("brokerName")));
      tmpConnectionFactory.setBrokerURL(String.format("vm://%s?create=false", objectName.getKeyProperty("brokerName")));

      RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
      redeliveryPolicy.setInitialRedeliveryDelay(1000);
      redeliveryPolicy.setMaximumRedeliveryDelay(60000);
      redeliveryPolicy.setBackOffMultiplier(1.5);
      redeliveryPolicy.setUseExponentialBackOff(true);
      redeliveryPolicy.setMaximumRedeliveries(-1);

      tmpConnectionFactory.setRedeliveryPolicy(redeliveryPolicy);

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
      if (hasSplunkSourcetype()) {
        builder.setSourcetype(splunkSourcetype);
      }

      queueListener.setMessageEventBuilder(builder);
      queueListener.setSplunkClient(splunkClient);

      queueListener.setDestinationName(objectName.getKeyProperty("destinationName"));

      if (listenerMap == null) {
        listenerMap = new ConcurrentHashMap<>();
      }

      log.info("Adding listener '{}' to map containing {}", objectNameString, listenerMap.keySet());
      listenerMap.put(objectNameString, queueListener);
    }

    return queueListener;
  }

  class ListenerStartupThread extends Thread {
    ObjectName mbeanName;

    public ListenerStartupThread(ObjectName objectName) {
      mbeanName = objectName;
      this.setName("ListenerStartupThread - " + objectName.getCanonicalName());
    }

    @Override
    public void run() {
      if (listenerMap.containsKey(mbeanName.getCanonicalName())) {
        log.info("Skipping creating MessageListener for '{}' - already create");
      } else {
        SplunkJmsMessageListener queueListener = createMessageListener(mbeanName);

        if (queueListener != null && !queueListener.isRunning()) {
          try {
            log.info("Sleeping before starting consumer");
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          queueListener.start();
          log.info("Consumer started");
        }
      }
    }
  }

}
