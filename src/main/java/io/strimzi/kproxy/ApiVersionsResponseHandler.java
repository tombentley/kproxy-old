/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.kproxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.strimzi.kproxy.codec.KafkaFrame;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ApiVersionsResponseHandler extends ChannelInboundHandlerAdapter {
    private static final Logger LOGGER = LogManager.getLogger(KafkaProxyFrontendHandler.class);
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        KafkaFrame frame = (KafkaFrame) msg;
        if (frame.apiKey() == ApiKeys.API_VERSIONS) {
            var resp = (ApiVersionsResponseData) frame.body();
            for (var key : resp.apiKeys()) {
                short apiId = key.apiKey();
                if (ApiKeys.hasId(apiId)) {
                    ApiKeys apiKey = ApiKeys.forId(apiId);
                    short min = (short) Math.max(
                            key.minVersion(),
                            apiKey.messageType.lowestSupportedVersion());
                    if (min != key.minVersion()) {
                        LOGGER.trace("[{}] Use {} min version {} (was: {})", ctx.channel(), apiKey, min, key.maxVersion());
                        key.setMinVersion(min);
                    }

                    short max = (short) Math.min(
                            key.maxVersion(),
                            apiKey.messageType.highestSupportedVersion());
                    if (max != key.maxVersion()) {
                        LOGGER.trace("[{}] Use {} max version {} (was: {})", ctx.channel(), apiKey, max, key.maxVersion());
                        key.setMaxVersion(max);
                    }
                }
            }
        }
        super.channelRead(ctx, msg);
    }
}
