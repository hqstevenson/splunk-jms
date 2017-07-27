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

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.EventCollectorClient;
import com.pronoia.splunk.jms.SplunkJmsMessageListener;
import com.pronoia.splunk.jms.activemq.internal.MessageListenerStartupTask;
import com.pronoia.splunk.jms.eventbuilder.JmsMessageEventBuilder;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
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
  EventBuilder<Message> splunkEventBuilder;

  long startupDelay = 15;
  TimeUnit startupDelayUnit = TimeUnit.SECONDS;

  boolean started = false;

  ScheduledExecutorService startupExecutor = Executors.newSingleThreadScheduledExecutor();

  Map<String, SplunkJmsMessageListener> listenerMap = new ConcurrentHashMap<>();

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

  public boolean hasSplunkEventBuilder() {
    return splunkEventBuilder != null;
  }

  public EventBuilder<Message> getSplunkEventBuilder() {
    return splunkEventBuilder;
  }

  public void setSplunkEventBuilder(EventBuilder<Message> splunkEventBuilder) {
    this.splunkEventBuilder = splunkEventBuilder;
  }

  public long getStartupDelay() {
    return startupDelay;
  }

  public void setStartupDelay(long startupDelay) {
    this.startupDelay = startupDelay;
  }

  public TimeUnit getStartupDelayUnit() {
    return startupDelayUnit;
  }

  public void setStartupDelayUnit(TimeUnit startupDelayUnit) {
    this.startupDelayUnit = startupDelayUnit;
  }

  public boolean isStarted() {
    return started;
  }

  public SplunkJmsMessageListener getMessageListener(String canonicalNameString) {
    return listenerMap.get(canonicalNameString);
  }

  void verifyConfiguration() throws IllegalStateException {
    if (!hasSplunkClient()) {
      throw new IllegalStateException("Splunk Client must be specified");
    }

    if (!hasSplunkEventBuilder()) {
      splunkEventBuilder = new JmsMessageEventBuilder();
      log.warn("Splunk EventBuilder<{}> is not specified - using default '{}'", Message.class.getName(), splunkEventBuilder.getClass().getName());
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
    if (isStarted()) {
      log.warn("Attempting to start a previously started {} - ignoring", this.getClass().getSimpleName());
      return;
    }

    verifyConfiguration();

    final ObjectName destinationObjectNamePattern = createDestinationObjectName();

    log.info("Starting {} with ObjectName pattern {}", this.getClass().getSimpleName(), destinationObjectNamePattern);

    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    MBeanServerNotificationFilter notificationFilter = new MBeanServerNotificationFilter();
    notificationFilter.enableAllObjectNames();
    notificationFilter.enableType(MBeanServerNotification.REGISTRATION_NOTIFICATION);

    log.debug("Looking for pre-existing destinations");
    Set<ObjectName> existingDestinationObjectNameSet = mbeanServer.queryNames(createDestinationObjectName(), null);
    if (existingDestinationObjectNameSet != null && !existingDestinationObjectNameSet.isEmpty()) {
      for(ObjectName mbeanName : existingDestinationObjectNameSet) {
        scheduleMessageListenerStartup(mbeanName);
      }
    }

    log.debug("Starting JMX NotificationListener watching for ObjectName pattern {}", this.getClass().getSimpleName(), destinationObjectNamePattern);
    try {
      mbeanServer.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this, notificationFilter, null);
    } catch (InstanceNotFoundException instanceNotFoundEx) {
      String errorMessage = String.format("Failed to add NotificationListener to '%s' with filter '%s' for '%s' - dynamic destination detection is disabled",
          MBeanServerDelegate.DELEGATE_NAME.getCanonicalName(),
          notificationFilter.toString(),
          destinationObjectNamePattern.getCanonicalName()
      );
      log.error(errorMessage, instanceNotFoundEx);
    }

    started = true;
  }

  /**
   * Stop the NotificationListener.
   */
  synchronized public void stop() {
    log.info("Stopping {} with ObjectName {}", this.getClass().getSimpleName(), createDestinationObjectName());

    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      mbeanServer.removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, this);
    } catch (InstanceNotFoundException | ListenerNotFoundException removalEx) {
      String errorMessage = String.format("Ignoring exception encountered removed NotificationListener from '%s'", MBeanServerDelegate.DELEGATE_NAME.toString());
      log.error(errorMessage, removalEx);
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
          log.debug("ObjectName '{}' matched '{}' - scheduling MessageListener startup.", mbeanName.getCanonicalName(), destinationObjectNamePattern.getCanonicalName());
          scheduleMessageListenerStartup(mbeanName);
        } else {
          log.trace("ObjectName '{}' did not match '{}' - ignoring.", mbeanName.getCanonicalName(), destinationObjectNamePattern.getCanonicalName());
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

  synchronized void scheduleMessageListenerStartup(ObjectName objectName) {
    String objectNameString = objectName.getCanonicalName();

    if (listenerMap.containsKey(objectNameString)) {
      log.debug("MessageListener startup already scheduled for {} - ignoring", objectNameString);
      return;
    }

    log.info("Scheduling MessageListener startup for {}", objectNameString);

    SplunkJmsMessageListener newMessageListener = new SplunkJmsMessageListener();

    ActiveMQConnectionFactory tmpConnectionFactory = new ActiveMQConnectionFactory();
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

    newMessageListener.setConnectionFactory(tmpConnectionFactory);
    newMessageListener.setMessageEventBuilder(splunkEventBuilder.duplicate());

    newMessageListener.setSplunkClient(splunkClient);

    newMessageListener.setDestinationName(objectName.getKeyProperty("destinationName"));

    if (!listenerMap.containsKey(objectNameString)) {
      log.info("Scheduling MessageListener for {}", objectNameString);
      listenerMap.put(objectNameString, newMessageListener);
      startupExecutor.schedule(new MessageListenerStartupTask(this, objectNameString), startupDelay, startupDelayUnit);
    }

    return;
  }

}
