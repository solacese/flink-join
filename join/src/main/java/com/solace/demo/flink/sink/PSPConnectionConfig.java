/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.solace.demo.flink.sink;

import com.solace.messaging.MessagingService;
import com.solace.messaging.PubSubPlusClientException;
import com.solace.messaging.config.SolaceProperties.AuthenticationProperties;
import com.solace.messaging.config.SolaceProperties.ServiceProperties;
import com.solace.messaging.config.SolaceProperties.TransportLayerProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

/**

 */
public class PSPConnectionConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PSPConnectionConfig.class);

    private static final long DEFAULT_DELIVERY_TIMEOUT = 30000;

    private String host;
    private String vpn = "default";
    private String username;
    private String password;
    private String uri;

    private transient MessagingService messagingService = null;

    /**
     * @param host host name
     * @param vpn virtual host
     * @param username username
     * @param password password
     * @throws NullPointerException if host or virtual host or username or password is null
     */
    private PSPConnectionConfig(
            String host,
            String vpn,
            String username,
            String password) {
        Preconditions.checkNotNull(host, "host can not be null");
        Preconditions.checkNotNull(vpn, "vpn can not be null");
        Preconditions.checkNotNull(username, "username can not be null");
        Preconditions.checkNotNull(password, "password can not be null");
        this.host = host;
        this.vpn = vpn;
        this.username = username;
        this.password = password;
    }

    /**
     * @param uri the connection URI
     * @throws NullPointerException if URI is null
     */
    private PSPConnectionConfig(
            String uri) {
        Preconditions.checkNotNull(uri, "Uri can not be null");
        this.uri = uri;
    }

    /** @return the host to use for connections */
    public String getHost() {
        return host;
    }

    /**
     * Retrieve the virtual host.
     *
     * @return the virtual host to use when connecting to the broker
     */
    public String getVpn() {
        return vpn;
    }

    /**
     * Retrieve the user name.
     *
     * @return the AMQP user name to use when connecting to the broker
     */
    public String getUsername() {
        return username;
    }

    /**
     * Retrieve the password.
     *
     * @return the password to use when connecting to the broker
     */
    public String getPassword() {
        return password;
    }

    /**
     * Retrieve the URI.
     *
     * @return the connection URI when connecting to the broker
     */
    public String getUri() {
        return uri;
    }

    /**
     */
    public MessagingService getMessagingService()
            throws PubSubPlusClientException {

        if(messagingService != null) {
            return messagingService;
        }

        final Properties props = new Properties();
        props.setProperty(TransportLayerProperties.HOST, this.host);
        props.setProperty(ServiceProperties.VPN_NAME, this.vpn);
        props.setProperty(AuthenticationProperties.SCHEME_BASIC_USER_NAME, this.username);
        props.setProperty(AuthenticationProperties.SCHEME_BASIC_PASSWORD, this.password);

        messagingService = MessagingService.builder(ConfigurationProfile.V1)
                .fromProperties(props)
                .build()
                .connect();
        return messagingService;
    }

    /** The Builder Class for {@link PSPConnectionConfig}. */
    public static class Builder {

        private String host;
        private String vpn;
        private String username;
        private String password;

        /**
         * @param host the default host to use for connections
         * @return the Builder
         */
        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        /**
         * Set the virtual host.
         *
         * @param vpn the virtual host to use when connecting to the broker
         * @return the Builder
         */
        public Builder setVpn(String vpn) {
            this.vpn = vpn;
            return this;
        }

        /**
         * Set the user name.
         *
         * @param username the AMQP user name to use when connecting to the broker
         * @return the Builder
         */
        public Builder setUserName(String username) {
            this.username = username;
            return this;
        }

        /**
         * Set the password.
         *
         * @param password the password to use when connecting to the broker
         * @return the Builder
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * The Builder method.
         *
         */
        public PSPConnectionConfig build() {
            return new PSPConnectionConfig(
                    this.host,
                    this.vpn,
                    this.username,
                    this.password);
        }
    }
}
