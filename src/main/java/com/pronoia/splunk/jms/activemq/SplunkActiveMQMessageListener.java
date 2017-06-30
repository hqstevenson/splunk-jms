package com.pronoia.splunk.jms.activemq;

import com.pronoia.splunk.jms.SplunkJmsMessageListener;

import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;

public class SplunkActiveMQMessageListener extends SplunkJmsMessageListener {
  String brokerURL;
  String userName;
  String password;

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
  protected Destination createDestination() throws JMSException {
    return ActiveMQDestination.createDestination(getDestinationName(), isUseQueue() ? ActiveMQDestination.QUEUE_TYPE : ActiveMQDestination.TOPIC_TYPE);
  }
}
