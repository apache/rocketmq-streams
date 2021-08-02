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
package org.apache.rocketmq.streams.script.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class UniqID {

    private static char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    private static UniqID me = new UniqID();

    private static final ThreadLocal<MessageDigest> digestLocal = new ThreadLocal<MessageDigest>();

    private static final SecureRandom random = new SecureRandom();

    private UniqID() {

    }

    public static UniqID getInstance() {
        return me;
    }

    public String getUniqIDHash() {
        byte[] randomBytes = new byte[16];
        random.nextBytes(randomBytes);

        MessageDigest digest = digestLocal.get();
        if (digest == null) {
            try {
                digest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("md5 algorithm not found.", e);
            }
            digestLocal.set(digest);
        }

        byte[] bt = digest.digest(randomBytes);
        int l = bt.length;

        char[] out = new char[l << 1];

        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = digits[(0xF0 & bt[i]) >>> 4];
            out[j++] = digits[0x0F & bt[i]];
        }

        return new String(out);
    }
}
