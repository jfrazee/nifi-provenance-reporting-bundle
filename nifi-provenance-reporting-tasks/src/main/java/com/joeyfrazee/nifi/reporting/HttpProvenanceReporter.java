/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.joeyfrazee.nifi.reporting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"http", "provenance"})
@CapabilityDescription("A provenance reporting task that posts to an HTTP server")
public class HttpProvenanceReporter extends AbstractProvenanceReporter {
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    public static final PropertyDescriptor URL = new PropertyDescriptor
            .Builder().name("URL")
            .displayName("URL")
            .description("The URL to post to")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    private final AtomicReference<OkHttpClient> client = new AtomicReference<>();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        descriptors = super.getSupportedPropertyDescriptors();
        descriptors.add(URL);
        return descriptors;
    }

    @OnScheduled
    public void setup(final ConfigurationContext context) throws IOException {
        client.set(new OkHttpClient());
    }

    private OkHttpClient getHttpClient() {
        return client.get();
    }

    private void post(String json, String url) throws IOException {
        RequestBody body = RequestBody.create(JSON, json);
        Request request = new Request.Builder()
            .url(url)
            .post(body)
            .build();
        Response response = getHttpClient().newCall(request).execute();
        getLogger().info("{} {} {}", response.code(), response.message(), response.body().string());
    }

    public boolean indexEvent(final Map<String, Object> event, final ReportingContext context) throws IOException {
        final String url = context.getProperty(URL).getValue();
        final String json = new ObjectMapper().writeValueAsString(event);
        post(json, url);
        return false;
    }
}
