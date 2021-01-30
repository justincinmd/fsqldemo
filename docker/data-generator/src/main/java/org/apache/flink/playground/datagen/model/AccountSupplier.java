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

package org.apache.flink.playground.datagen.model;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/** A supplier that generates an arbitrary transaction. */
public class AccountSupplier implements Supplier<Account> {

    private final Random generator = new Random();

    private final Iterator<Long> accounts =
            Stream.generate(() -> Stream.of(1L, 2L, 3L, 4L, 5L))
                    .flatMap(UnaryOperator.identity())
                    .iterator();

    private final Iterator<String> accountNames =
            Stream.generate(() -> Stream.of("Account 1", "Account 2", "Account 3", "Account 4", "Account 5"))
                    .flatMap(UnaryOperator.identity())
                    .iterator();

    private final Iterator<LocalDateTime> timestamps =
            Stream.iterate(
                    LocalDateTime.of(2000, 1, 1, 1, 0),
                    time -> time.plusMinutes(5).plusSeconds(generator.nextInt(58) + 1))
                    .iterator();

    @Override
    public Account get() {
        Account account = new Account();
        account.accountId = accounts.next();
        account.accountName = accountNames.next();
        account.timestamp = timestamps.next();

        return account;
    }
}
