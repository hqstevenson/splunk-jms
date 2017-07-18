package com.pronoia.splunk.jms.activemq.internal;

import com.pronoia.splunk.jms.SplunkJmsMessageListener;
import com.pronoia.splunk.jms.activemq.SplunkEmbeddedActiveMQMessageListenerFactory;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageListenerStartupTask implements Runnable {
  Logger log = LoggerFactory.getLogger(this.getClass());

  SplunkEmbeddedActiveMQMessageListenerFactory listenerFactory;
  String canonicalNameString;

  public MessageListenerStartupTask(SplunkEmbeddedActiveMQMessageListenerFactory listenerFactory, String canonicalNameString) {
    this.listenerFactory = listenerFactory;
    this.canonicalNameString = canonicalNameString;
  }

  @Override
  public void run() {
    log.info("Preparing to start {}", canonicalNameString);

    SplunkJmsMessageListener messageListener = listenerFactory.getMessageListener(canonicalNameString);
    if (messageListener != null && !messageListener.isRunning()) {
      messageListener.start();
    }
  }

}