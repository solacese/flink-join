/*
 * Copyright 2021-2023 Solace Corporation. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.solace.demo.util;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceProperties.AuthenticationProperties;
import com.solace.messaging.config.SolaceProperties.ServiceProperties;
import com.solace.messaging.config.SolaceProperties.TransportLayerProperties;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.resources.Topic;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A more performant sample that shows an application that publishes.
 */
public class DirectPublisher {
    
    private static final String SAMPLE_NAME = DirectPublisher.class.getSimpleName();
    private static final String TOPIC_PREFIX = "solace/samples/";  // used as the topic "root"
    private static final String API = "Java";
    private static final int APPROX_MSG_RATE_PER_SEC = 1;
    private static final int PAYLOAD_SIZE = 100;
    private static final int MAX_KEYS = 100;
    
    private static volatile int msgSentCounter = 0;                   // num messages sent
    private static volatile boolean isShutdown = false;
    private static volatile boolean useTimestamps = false;

    /** Main method. */
    public static void main(String... args) throws IOException {
        if (args.length < 3) {  // Check command line arguments
            System.out.printf("Usage: %s <host:port> <message-vpn> <client-username> [password] [with-timestamps] %n%n", SAMPLE_NAME);
            System.exit(-1);
        }
        System.out.println(API + " " + SAMPLE_NAME + " initializing...");

        final Properties properties = new Properties();
        properties.setProperty(TransportLayerProperties.HOST, args[0]);          // host:port
        properties.setProperty(ServiceProperties.VPN_NAME,  args[1]);     // message-vpn
        properties.setProperty(AuthenticationProperties.SCHEME_BASIC_USER_NAME, args[2]);      // client-username
        if (args.length > 3) {
            properties.setProperty(AuthenticationProperties.SCHEME_BASIC_PASSWORD, args[3]);  // client-password
        }

        // timestamps?
        if (args.length > 4) {
            String uts = args[4].toLowerCase();
            useTimestamps = uts.equals("true") || uts.equals("yes");
        }
        //properties.setProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS, true);  // not required, but interesting
        properties.setProperty(TransportLayerProperties.RECONNECTION_ATTEMPTS, "20");  // recommended settings
        properties.setProperty(TransportLayerProperties.CONNECTION_RETRIES_PER_HOST, "5");

        final MessagingService messagingService = MessagingService.builder(ConfigurationProfile.V1)
                .fromProperties(properties)
                .build();
        messagingService.connect();  // blocking connect
        messagingService.addServiceInterruptionListener(serviceEvent -> {
            System.out.println("### SERVICE INTERRUPTION: "+serviceEvent.getCause());
            //isShutdown = true;
        });
        messagingService.addReconnectionAttemptListener(serviceEvent -> {
            System.out.println("### RECONNECTING ATTEMPT: "+serviceEvent);
        });
        messagingService.addReconnectionListener(serviceEvent -> {
            System.out.println("### RECONNECTED: "+serviceEvent);
        });
        
        // build the publisher object
        final DirectMessagePublisher publisher = messagingService.createDirectMessagePublisherBuilder()
                .onBackPressureWait(1)
                .build();
        publisher.start();
        
        // can be called for ACL violations, 
        publisher.setPublishFailureListener(e -> {
            System.out.println("### FAILED PUBLISH "+e);
        });
        
        // make a thread for printing message rate stats
        ScheduledExecutorService statsPrintingThread = Executors.newSingleThreadScheduledExecutor();
        statsPrintingThread.scheduleAtFixedRate(() -> {
            System.out.printf("Published msgs/s: %,d%n",msgSentCounter);  // simple way of calculating message rates
            msgSentCounter = 0;
        }, 1, 1, TimeUnit.SECONDS);

        System.out.println(API + " " + SAMPLE_NAME + " connected, and running. Press [ENTER] to quit.");

        int key;    // our message's key
        int dest_q; // where do you wanna go?
        int[] counter = new int[MAX_KEYS];

        // messge template
        final String message = "{\"key\": %d, \"message\":\"message # %d for key %d\"}";
        final String tsmessage = "{\"key\": %d, \"message\":\"message # %d for key %d\", \"ts\": %d}";

        OutboundMessageBuilder messageBuilder = messagingService.messageBuilder();
        // block the main thread, waiting for a quit signal
        while (System.in.available() == 0 && !isShutdown) {
            try {
                // each loop, change the payload, less trivial
                key = (int) (Math.random() * MAX_KEYS);
                dest_q = (int) (Math.random() * 2); // should be 0 or 1
                counter[key]++;

                String msg; // insert values into message
                if(useTimestamps)
                    msg = String.format(tsmessage, key, counter[key], key, System.currentTimeMillis());
                else
                    msg = String.format(tsmessage, key, counter[key], key);

                System.out.println("message is " + msg);
                OutboundMessage om = messageBuilder.build(msg.getBytes());
                // dynamic topics!!
                String topicString = new StringBuilder(TOPIC_PREFIX)
                        .append(dest_q).toString();
                publisher.publish(om,Topic.of(topicString));  // send the message
                msgSentCounter++;  // add one
            } catch (RuntimeException e) {  // threw from publish(), only thing that is throwing here
                System.out.printf("### Caught while trying to publisher.publish(): %s%n",e);
                isShutdown = true;  // or try to handle the specific exception more gracefully
            } finally {
                try {
                    Thread.sleep(1000 / APPROX_MSG_RATE_PER_SEC);  // do Thread.sleep(0) for max speed
                    // Note: STANDARD Edition Solace PubSub+ broker is limited to 10k msg/s max ingress
                } catch (InterruptedException e) {
                    isShutdown = true;
                }
            }
        }
        isShutdown = true;
        statsPrintingThread.shutdown();  // stop printing stats
        publisher.terminate(500);
        messagingService.disconnect();
        System.out.println("Main thread quitting.");
    }
}
