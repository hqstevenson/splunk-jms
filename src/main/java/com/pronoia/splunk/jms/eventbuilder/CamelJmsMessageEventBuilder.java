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
package com.pronoia.splunk.jms.eventbuilder;

import javax.jms.Message;

import com.pronoia.splunk.eventcollector.EventBuilder;
import com.pronoia.splunk.eventcollector.eventbuilder.EventBuilderSupport;

public class CamelJmsMessageEventBuilder extends JmsMessageEventBuilder {
    public CamelJmsMessageEventBuilder() {
        setPropertyNameReplacement("_DOT_", ".");
        setPropertyNameReplacement("_HYPHEN_", "-");
    }

    @Override
    public EventBuilder<Message> duplicate() {
        log.trace("Duplicating CamelJmsMessageEventBuilder");

        CamelJmsMessageEventBuilder answer = new CamelJmsMessageEventBuilder();

        answer.copyConfiguration(this);

        return answer;
    }

    @Override
    protected void copyConfiguration(EventBuilderSupport<Message> sourceEventBuilder) {
        log.trace("copyConfiguration CamelJmsMessageEventBuilder");
        super.copyConfiguration(sourceEventBuilder);
    }

}
