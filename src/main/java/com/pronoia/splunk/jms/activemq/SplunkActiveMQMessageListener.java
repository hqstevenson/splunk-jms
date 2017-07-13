package com.pronoia.splunk.jms.activemq;

import com.pronoia.splunk.jms.SplunkJmsMessageListener;
import com.pronoia.splunk.jms.builder.JmsMessageEventBuilder;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;

public class SplunkActiveMQMessageListener extends SplunkJmsMessageListener {
  String brokerURL;
  String userName;
  String password;

  String splunkIndex;
  String splunkSource;
  String splunkSourcetype;

  public boolean hasBrokerURL() {
    return brokerURL != null && !brokerURL.isEmpty();
  }

  public String getBrokerURL() {
    return brokerURL;
  }

  public void setBrokerURL(String brokerURL) {
    this.brokerURL = brokerURL;
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

  @Override
  public void verifyConfiguration() {
    if (!hasBrokerURL()) {
      throw new IllegalStateException("ActiveMQ Broker URL must be specified");
    }
    ActiveMQConnectionFactory tmpConnectionFactory = new ActiveMQConnectionFactory();
    tmpConnectionFactory.setBrokerURL(brokerURL);

    if (hasUserName()) {
      tmpConnectionFactory.setUserName(userName);
    }

    if (hasPassword()) {
      tmpConnectionFactory.setPassword(password);
    }

    this.setConnectionFactory(tmpConnectionFactory);

    super.verifyConfiguration();
  }

  @Override
  public void start() {
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

    setMessageEventBuilder(builder);

    super.start();
  }

  @Override
  protected Destination createDestination() throws JMSException {
    return ActiveMQDestination.createDestination(getDestinationName(), isUseTopic() ? ActiveMQDestination.QUEUE_TYPE : ActiveMQDestination.TOPIC_TYPE);
  }
}
