/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.tuner;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/** POJO to parse and store the JVM option from jvm.options file. Created by aagrawal on 8/28/17. */
public class JVMOption {
    private String jvmOption;
    private String value;
    private boolean isCommented;
    private boolean isHeapJVMOption;
    private static final Pattern pattern = Pattern.compile("(#)*(-[^=]+)=?(.*)?");
    // A new pattern is required because heap do not separate JVM key,value with "=".
    private static final Pattern heapPattern =
            Pattern.compile(
                    "(#)*(-Xm[x|s|n])([0-9]+[K|M|G])?"); // Pattern.compile("(#)*-(Xm[x|s|n])([0-9]+)(K|M|G)?");

    public JVMOption(String jvmOption) {
        this.jvmOption = jvmOption;
    }

    public JVMOption(String jvmOption, String value, boolean isCommented, boolean isHeapJVMOption) {
        this.jvmOption = jvmOption;
        this.value = value;
        this.isCommented = isCommented;
        this.isHeapJVMOption = isHeapJVMOption;
    }

    public String toJVMOptionString() {
        final StringBuilder sb = new StringBuilder();
        if (isCommented) sb.append("#");
        sb.append(jvmOption);
        if (value != null) {
            if (!isHeapJVMOption) sb.append("=");
            sb.append(value);
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JVMOption jvmOption1 = (JVMOption) o;
        return isCommented == jvmOption1.isCommented
                && isHeapJVMOption == jvmOption1.isHeapJVMOption
                && Objects.equals(jvmOption, jvmOption1.jvmOption)
                && Objects.equals(value, jvmOption1.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jvmOption, value, isCommented, isHeapJVMOption);
    }

    public String getJvmOption() {

        return jvmOption;
    }

    public JVMOption setJvmOption(String jvmOption) {
        this.jvmOption = jvmOption.trim();
        return this;
    }

    public String getValue() {
        return value;
    }

    public JVMOption setValue(String value) {
        if (!StringUtils.isEmpty(value)) this.value = value;
        return this;
    }

    public boolean isCommented() {
        return isCommented;
    }

    public JVMOption setCommented(boolean commented) {
        isCommented = commented;
        return this;
    }

    public boolean isHeapJVMOption() {
        return isHeapJVMOption;
    }

    public JVMOption setHeapJVMOption(boolean heapJVMOption) {
        isHeapJVMOption = heapJVMOption;
        return this;
    }

    public static JVMOption parse(String line) {
        JVMOption result = null;

        // See if it is heap JVM option.
        Matcher matcher = heapPattern.matcher(line);
        if (matcher.matches()) {
            boolean isCommented = (matcher.group(1) != null);
            return new JVMOption(matcher.group(2))
                    .setCommented(isCommented)
                    .setValue(matcher.group(3))
                    .setHeapJVMOption(true);
        }

        // See if other heap option.
        matcher = pattern.matcher(line);
        if (matcher.matches()) {
            boolean isCommented = (matcher.group(1) != null);
            return new JVMOption(matcher.group(2))
                    .setCommented(isCommented)
                    .setValue(matcher.group(3));
        }

        return result;
    }
}
