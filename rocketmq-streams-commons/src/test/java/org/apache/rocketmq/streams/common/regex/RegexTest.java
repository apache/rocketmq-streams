package org.apache.rocketmq.streams.common.regex;

import com.google.re2j.Pattern;
import java.util.Set;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.optimization.RegexEngine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RegexTest {

    @Before
    public void init() {
        ComponentCreator.getProperties().setProperty(ConfigureFileKey.DIPPER_REGEX_ENGINE, "re2j");
    }

    @Test
    public void testRe2Basic() {
        RegexEngine<Integer> regexEngine = new RegexEngine<>();
        regexEngine.addRegex("python", 0);
        regexEngine.addRegex("\\.dll", 1);
        String message = "python test.py";
        boolean matched = regexEngine.match(message);
        Assert.assertEquals(true, matched);
        message = "test.dll";
        Set<Integer> matchedSet = regexEngine.matchExpression(message);
        Assert.assertEquals(1, matchedSet.size());
        Assert.assertEquals(1, java.util.Optional.ofNullable(matchedSet.iterator().next()).get().intValue());
    }

    @Test
    public void testRe2UnSupport() {
        String regex = "(?P<P570>(^\\\\\"[^\\\\\"]+\\\\[^\\\\\\\\\"]*\\\\[^\\\\\\\\\"]*\\?\\w{3,4}(?=\\.)[^\\\\\\\\\"]*(?<=\\.)\\w{2,4}\\\\\"))";
        try {
            Pattern pattern = Pattern.compile(regex, 4);
        } catch (Exception e) {
            e.printStackTrace();
        }
        regex = "/(script|ttyrec|rootsh|sniffy|ttyrpld|ttysnoop|gmond)$";
        try {
            Pattern pattern = Pattern.compile(regex, 4);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
