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
package org.apache.rocketmq.streams.common.optimization;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 可以用sql中的like表示正则，系统负责完成转化
 */
public class LikeRegex {
    private static final Log LOG = LogFactory.getLog(LikeRegex.class);
    public static final String SPECAIL_WORD = "%";
    public static String[] regexSpecialWords = {"\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|"};

    protected String likeStr;
    protected boolean isStartFlag = true;
    protected boolean isEndFlag = true;
    protected List<String> quickMatchWord = new ArrayList<>();

    public LikeRegex(String likeStr) {
        this.likeStr = likeStr;
        parse();
    }

    public void parse() {
        String tmp = likeStr;
        if (tmp == null) {
            return;
        }
        if (tmp.startsWith(SPECAIL_WORD)) {
            isStartFlag = false;
            tmp = tmp.substring(1);
        }
        if (tmp.endsWith(SPECAIL_WORD)) {
            isEndFlag = false;
            tmp = tmp.substring(0, tmp.length() - 1);
        }
        if (!tmp.contains(SPECAIL_WORD)) {
            quickMatchWord.add(tmp);
            return;
        }
        String[] words = tmp.split(SPECAIL_WORD);
        for (int i = 0; i < words.length; i++) {
            quickMatchWord.add(words[i]);
        }
    }

    public boolean match(String content) {
        if (content == null) {
            return false;
        }
        if (quickMatchWord == null || quickMatchWord.size() == 0) {
            LOG.warn("like may be parse error, words is empty " + likeStr);
            return false;
        }
        for (int i = 0; i < quickMatchWord.size(); i++) {
            String word = quickMatchWord.get(i);
            if (i == 0 && isStartFlag) {
                if (!startsWith(content.toCharArray(), word, 0)) {
                    return false;
                }

            }
            if (i == quickMatchWord.size() - 1 && isEndFlag) {
                if (!startsWith(content.toCharArray(), word, content.length() - word.length())) {
                    return false;
                }
            }
            if (!contains(content, word)) {
                return false;
            }
        }
        return true;
    }

    public boolean startsWith(char[] content, String prefix, int toffset) {
        char ta[] = content;
        int to = toffset;
        char pa[] = prefix.toCharArray();
        int po = 0;
        int pc = pa.length;
        // Note: toffset might be near -1>>>1.
        if ((toffset < 0) || (toffset > content.length - pc)) {
            return false;
        }
        while (--pc >= 0) {
            if (pa[po] == '_') {
                to++;
                po++;
                continue;
            }
            if (ta[to++] != pa[po++]) {
                return false;
            }
        }
        return true;
    }

    public boolean contains(String m, String s) {
        return indexOf(m, s) > -1;
    }

    public int indexOf(String mainStr, String str) {
        return indexOf(mainStr.toCharArray(), str, 0);
    }

    public int indexOf(char[] value, String str, int fromIndex) {
        return indexOf(value, 0, value.length,
            str.toCharArray(), 0, str.length(), fromIndex);
    }

    public int indexOf(char[] source, int sourceOffset, int sourceCount,
                       char[] target, int targetOffset, int targetCount,
                       int fromIndex) {
        if (fromIndex >= sourceCount) {
            return (targetCount == 0 ? sourceCount : -1);
        }
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        if (targetCount == 0) {
            return fromIndex;
        }

        char first = target[targetOffset];
        int max = sourceOffset + (sourceCount - targetCount);

        for (int i = sourceOffset + fromIndex; i <= max; i++) {
            /* Look for first character. */
            if (source[i] != first && first != '_') {
                while (++i <= max && source[i] != first && first != '_') { ; }
            }

            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = targetOffset + 1; j < end && (source[j]
                    == target[k] || target[k] == '_'); j++, k++) { ; }

                if (j == end) {
                    /* Found whole string. */
                    return i - sourceOffset;
                }
            }
        }
        return -1;
    }

    public static void main(String[] args) {
        String content = "xCurrentVersion\\Windows\\load";
        String likeStr = "_Current_ersion\\Windows\\load%";
        LikeRegex likeRegex = new LikeRegex(likeStr);
        likeRegex.parse();
        System.out.println(likeRegex.match(content));
    }
}
