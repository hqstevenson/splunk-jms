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
package com.pronoia.splunk.jms;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * Configuration Tests for the SplunkJmsMessageListener class.
 */
public class SplunkJmsConsumerSupportConfigurationTest {
    static final String DESTINATION_NAME = "myDestinationName";

    SplunkJmsConsumerSupport instance;

    @Before
    public void setUp() throws Exception {
        instance = new SplunkJmsConsumerSupport(DESTINATION_NAME);
    }

    /**
     * @throws Exception in the event of a test error.
     */
    @Test
    public void testGetDestinationName() throws Exception {
        assertEquals(DESTINATION_NAME, instance.getDestinationName());
    }

    /**
     * @throws Exception in the event of a test error.
     */
    @Ignore("SplunkJmsConsumerSupportConfigurationTest.testVerifyConfiguration method not yet implemented")
    @Test
    public void testVerifyConfiguration() throws Exception {
        throw new UnsupportedOperationException("SplunkJmsConsumerSupportConfigurationTest.testVerifyConfiguration not yet implemented");
    }

}