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


import com.cloudhopper.smpp.pdu.SubmitSm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by Andreas on 28-09-2015.
 */
public class Db {

    private static final Logger logger = LoggerFactory.getLogger(Db.class);

    protected static Connection getConnection() throws SQLException {
        if (connection == null) {
            String host = "192.168.12.66";
            String port = "3306";
            String database = "ch_log";
            String username = "user";
            String password = "pass";
            String dbUrl = "jdbc:mysql://" + host + ":" + port + "/" + database;
            connection = DriverManager.getConnection(dbUrl, username, password);
        }
        return connection;
    }

    private static Connection connection;

    private static Db instance;

    public static Db get() {
        if (instance == null) {
            instance = new Db();
        }
        return instance;
    }

    private Db() {
        runner.start();
    }

    public void log(SubmitSm sm) {
        runner.add(sm);
        Connection conn;
        try {
            conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO log (source, dest, message) values (?,?,?)");
            int i = 1;
            stmt.setString(i++, sm.getSourceAddress().toString());
            stmt.setString(i++, sm.getDestAddress().toString());
            stmt.setString(i++, new String(sm.getShortMessage(), "UTF-8"));

            stmt.execute();

        } catch (SQLException ex) {
            logger.error("Could not save", ex);
        } catch (UnsupportedEncodingException ex) {
            logger.error("Could not save", ex);
        }
    }

    DbRunner runner = new DbRunner();

    public void stop() {
        runner.stopT();
    }

    public static class DbRunner extends Thread {
        public boolean running;
        Queue<SubmitSm> queue = new LinkedList<SubmitSm>();

        public synchronized void add(SubmitSm sm) {
            queue.add(sm);
        }

        private SubmitSm get() {
            return queue.poll();
        }

        @Override
        public void run() {
            running = true;
            while (running) {
                SubmitSm sm = get();
                if (sm != null) {
                    logMessage(sm);
                }
            }
        }

        public void stopT() {
            running = false;
        }

        private void logMessage(SubmitSm sm) {
            PreparedStatement stmt = null;
            try {
                Connection conn = getConnection();
                stmt = conn.prepareStatement("INSERT INTO log (source, dest, message) values (?,?,?)");
                int i = 1;
                stmt.setString(i++, sm.getSourceAddress().toString());
                stmt.setString(i++, sm.getDestAddress().toString());
                stmt.setString(i++, new String(sm.getShortMessage(), "UTF-8"));

                stmt.execute();

            } catch (SQLException ex) {
                logger.error("Could not save", ex);
            } catch (UnsupportedEncodingException ex) {
                logger.error("Could not save", ex);
            } finally {
                try {
                    stmt.close();
                } catch (SQLException e) {
                }
            }
        }
    };
}
