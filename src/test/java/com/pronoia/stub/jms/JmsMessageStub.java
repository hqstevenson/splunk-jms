/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pronoia.stub.jms;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

public class JmsMessageStub implements Message {
    String id = "Dummy Message Id";
    long timestamp = System.currentTimeMillis();
    byte[] correlationIdBytes;
    String correlationId;
    Destination destination;
    Destination replyTo;
    int deliveryMode;
    boolean redelivered;
    String type;
    long expiration;
    int priority = 4;

    Map<String, Object> properties = new HashMap<>();

    @Override
    public String getJMSMessageID() throws JMSException {
        return id;
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {
        this.id = id;
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return timestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return correlationIdBytes;
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        this.correlationIdBytes = correlationID;
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        this.correlationId = correlationID;
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return this.correlationId;
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        this.replyTo = replyTo;
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        this.destination = destination;
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return deliveryMode;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return redelivered;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        this.redelivered = redelivered;
    }

    @Override
    public String getJMSType() throws JMSException {
        return type;
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        this.type = type;
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return expiration;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        this.expiration = expiration;
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return priority;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        this.priority = priority;
    }

    @Override
    public void clearProperties() throws JMSException {
        properties.clear();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return properties.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return Boolean.parseBoolean(getStringProperty(name));
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return Byte.parseByte(getStringProperty(name));
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return Short.parseShort(getStringProperty(name));
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return Integer.parseInt(getStringProperty(name));
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return Long.parseLong(getStringProperty(name));
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return Float.parseFloat(getStringProperty(name));
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return Double.parseDouble(getStringProperty(name));
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return properties.containsKey(name) ? properties.get(name).toString() : null;
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return properties.getOrDefault(name, null);
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException {
        return Collections.enumeration(properties.keySet());
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        properties.put(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        properties.put(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        properties.put(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        properties.put(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        properties.put(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        properties.put(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        properties.put(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        properties.put(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        properties.put(name, value);
    }

    @Override
    public void acknowledge() throws JMSException {
    }

    @Override
    public void clearBody() throws JMSException {
    }

}
