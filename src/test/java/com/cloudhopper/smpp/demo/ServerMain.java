package com.cloudhopper.smpp.demo;

/*
 * #%L
 * ch-smpp
 * %%
 * Copyright (C) 2009 - 2015 Cloudhopper by Twitter
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudhopper.commons.util.windowing.DuplicateKeyException;
import com.cloudhopper.commons.util.windowing.OfferTimeoutException;
import com.cloudhopper.smpp.SmppConstants;
import com.cloudhopper.smpp.SmppServerConfiguration;
import com.cloudhopper.smpp.SmppServerHandler;
import com.cloudhopper.smpp.SmppServerSession;
import com.cloudhopper.smpp.SmppSession;
import com.cloudhopper.smpp.SmppSessionConfiguration;
import com.cloudhopper.smpp.impl.DefaultSmppServer;
import com.cloudhopper.smpp.impl.DefaultSmppSessionHandler;
import com.cloudhopper.smpp.pdu.BaseBind;
import com.cloudhopper.smpp.pdu.BaseBindResp;
import com.cloudhopper.smpp.pdu.PduRequest;
import com.cloudhopper.smpp.pdu.PduResponse;
import com.cloudhopper.smpp.pdu.SubmitSm;
import com.cloudhopper.smpp.pdu.SubmitSmResp;
import com.cloudhopper.smpp.type.SmppProcessingException;

/**
 *
 * @author joelauer (twitter: @jjlauer or <a href="http://twitter.com/jjlauer" target=window>http://twitter.com/jjlauer</a>)
 */
public class ServerMain {
    private static final Logger logger = LoggerFactory.getLogger(ServerMain.class);

    private static final AtomicInteger requestCounter = new AtomicInteger();

    static public void main(String[] args) throws Exception {
        //
        // setup 3 things required for a server
        //

        // for monitoring thread use, it's preferable to create your own instance
        // of an executor and cast it to a ThreadPoolExecutor from Executors.newCachedThreadPool()
        // this permits exposing things like executor.getActiveCount() via JMX possible
        // no point renaming the threads in a factory since underlying Netty 
        // framework does not easily allow you to customize your thread names
        ThreadPoolExecutor executor = (ThreadPoolExecutor)Executors.newCachedThreadPool();

        // to enable automatic expiration of requests, a second scheduled executor
        // is required which is what a monitor task will be executed with - this
        // is probably a thread pool that can be shared with between all client bootstraps
        ScheduledThreadPoolExecutor monitorExecutor = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private AtomicInteger sequence = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("SmppServerSessionWindowMonitorPool-" + sequence.getAndIncrement());
                return t;
            }
        });

        Db db = Db.get();

        // create a server configuration
        SmppServerConfiguration configuration = new SmppServerConfiguration();
        configuration.setPort(Integer.parseInt(args[0]));
        configuration.setMaxConnectionSize(10);
        configuration.setNonBlockingSocketsEnabled(true);
        configuration.setDefaultRequestExpiryTimeout(30000);
        configuration.setDefaultWindowMonitorInterval(15000);
        configuration.setDefaultWindowSize(50);
        configuration.setDefaultWindowWaitTimeout(configuration.getDefaultRequestExpiryTimeout());
        configuration.setDefaultSessionCountersEnabled(true);
        configuration.setJmxEnabled(true);

        // create a server, start it up
        DefaultSmppServer smppServer = new DefaultSmppServer(configuration, new DefaultSmppServerHandler(db), executor, monitorExecutor);

        logger.info("Starting SMPP server...");
//        new TrafficWatcherThread().start();
        smppServer.start();
        logger.info("SMPP server started");

        System.out.println("Press any key to stop server");
        System.in.read();

        logger.info("Stopping SMPP server...");
        smppServer.stop();
        db.stop();
        logger.info("SMPP server stopped");

        logger.info("Server counters: {}", smppServer.getCounters());
    }

    public static class DefaultSmppServerHandler implements SmppServerHandler {

        private final Db db;

        public DefaultSmppServerHandler(Db db) {
            this.db = db;
        }

        @Override
        public void sessionBindRequested(Long sessionId, SmppSessionConfiguration sessionConfiguration, final BaseBind bindRequest) throws SmppProcessingException {
            // test name change of sessions
            // this name actually shows up as thread context....
            sessionConfiguration.setName("Application.SMPP." + sessionConfiguration.getSystemId());

            //throw new SmppProcessingException(SmppConstants.STATUS_BINDFAIL, null);
        }

        @Override
        public void sessionCreated(Long sessionId, SmppServerSession session, BaseBindResp preparedBindResponse) throws SmppProcessingException {
            logger.info("Session created: {}", session);
            // need to do something it now (flag we're ready)
            session.serverReady(new TestSmppSessionHandler(session, db));
        }

        @Override
        public void sessionDestroyed(Long sessionId, SmppServerSession session) {
            logger.info("Session destroyed: {}", session);
            // print out final stats
            if (session.hasCounters()) {
                logger.info(" final session rx-submitSM: {}", session.getCounters().getRxSubmitSM());
            }

            // make sure it's really shutdown
            session.destroy();
        }

    }

    private static int messageId = 0;
    private static synchronized int getMessageId() {
        return ++messageId;
    }

    public static class TestSmppSessionHandler extends DefaultSmppSessionHandler {

        private WeakReference<SmppSession> sessionRef;
        private Db db;

        public TestSmppSessionHandler(SmppSession session, Db db) {
            this.sessionRef = new WeakReference<SmppSession>(session);
            this.db = db;
        }

        @Override
        public PduResponse firePduRequestReceived(PduRequest pduRequest) {
            SmppSession session = sessionRef.get();

            // mimic how long processing could take on a slower smsc
            try {
                Thread.sleep(25);
            } catch (Exception e) { }

            requestCounter.incrementAndGet();

            if (pduRequest.getCommandId() == SmppConstants.CMD_ID_SUBMIT_SM) {
                SubmitSm mt = (SubmitSm) pduRequest;

                logger.debug(pduRequest.toString());
                try {
                    logger.info("MESSAGE: " + new String(mt.getShortMessage(), "UTF-8"));
                } catch (UnsupportedEncodingException ex) {}

                SubmitSmResp response = mt.createResponse();
                int messageId = getMessageId();
                response.setMessageId("" + messageId);
                logger.debug("messageId: " + messageId);

                try {
                    db.log(mt);
                } catch (Exception ex) {
                    logger.error("Could not log", ex);
                }

//                if (Math.random() < 0.01) {
//                    logger.warn("Simulating timeout on messageId: " + messageId);
//                    try {
//                        Thread.sleep(60000);
//                    } catch (Exception e) { }
//                }


                return response;
            }

//            if (Math.random() < 0.01) {
//                logger.warn("Simulating enquery timeout...");
//                try {
//                    Thread.sleep(60000);
//                } catch (Exception e) { }
//            }

            return pduRequest.createResponse();
        }
    }

    private static class TrafficWatcherThread extends Thread {
        @Override
        public void run() {
            logger.info("Starting traffic watcher...");
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                int trafficPerSecond = requestCounter.getAndSet(0);
                if (trafficPerSecond > 0) {
                    logger.warn("Traffic per second : " + trafficPerSecond);
                }
            }
        }
    }
}
