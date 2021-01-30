/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playgrounds.spendreport;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class SpendReport {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // TODO: Create tables dynamically based on passed-in configuration
        //       and consumer schemas.
        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3)\n," +
                "    PRIMARY KEY (account_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'key.format'    = 'raw',\n" +
                "    'value.format'    = 'csv'\n" +
                ")");
        // SIDE NOTE: You get radically different output depending on if upsert-kafka
        // or kafka is used as a source. With upsert-kafka, all msgs are upserts,
        // with kafka, all messages are inserts (e.g. unique pk). Some SQL (e.g. equijoin)
        // needs all messages to be upserts to avoid unbounded state growth. See PLAIN_KAFKA
        // for an example of how to create a plain kafka table.
        tEnv.executeSql("CREATE TABLE accounts (\n" +
                "    account_id  BIGINT,\n" +
                "    account_name      STRING,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    PRIMARY KEY (account_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'upsert-kafka',\n" +
                "    'topic'     = 'accounts',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'key.format'    = 'raw',\n" +
                "    'value.format'    = 'csv'\n" +
                ")");

        // The easiest way to understand what's going on here is to run
        // with each of the below select statements so you can see what's going
        // in transactions, accounts, and the joined table.

        // NOTE: 100 transactions are emitted per second, one each for all 5 accts
        // TableResult tr = tEnv.sqlQuery("SELECT * FROM transactions").execute();

        // NOTE: 4 accounts are emitted only once. One account is updated for
        // every 100 transactions with a new account_name.
        // TableResult tr = tEnv.sqlQuery("SELECT * FROM accounts").execute();

        // TODO: Make this SQL passed in through configuration
        // NOTE: The SQL isn't actually interpreted until runtime, so a single configurable
        // jar can run any SQL on the provided tables if passed in. In other words,
        // the topo is dynamic.
        // MODIFY THIS LINE, THEN docker-compose build, docker-compose up to run.
        TableResult tr = tEnv.sqlQuery("SELECT transactions.account_id, account_name, amount FROM transactions INNER JOIN accounts ON transactions.account_id = accounts.account_id").execute();

        // TODO: Replace this with Kafka Output - print is useful for testing sql
        tr.print();
    }
}
