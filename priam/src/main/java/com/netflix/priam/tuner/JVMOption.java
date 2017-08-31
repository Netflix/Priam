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

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * POJO to parse and store the JVM option from jvm.options file.
 * Created by aagrawal on 8/28/17.
 */
public class JVMOption {
    private String jvmOption;
    private String value;
    private boolean isCommented;
    private static final Pattern pattern = Pattern.compile("(#)*(-[^=]+)=?(.*)?");

    public JVMOption(String jvmOption) {
        this.jvmOption = jvmOption;
    }

    public JVMOption(String jvmOption, String value, boolean isCommented) {
        this.jvmOption = jvmOption;
        this.value = value;
        this.isCommented = isCommented;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        if (isCommented)
            sb.append("#");
        sb.append(jvmOption);
        if (value != null)
            sb.append("=").append(value);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JVMOption jvmOption1 = (JVMOption) o;
        return isCommented == jvmOption1.isCommented &&
                Objects.equals(jvmOption, jvmOption1.jvmOption) &&
                Objects.equals(value, jvmOption1.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jvmOption, value, isCommented);
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
        if (!StringUtils.isEmpty(value))
            this.value = value;
        return this;
    }

    public boolean isCommented() {
        return isCommented;
    }

    public JVMOption setCommented(boolean commented) {
        isCommented = commented;
        return this;
    }

    public static JVMOption parse(String line){
        JVMOption result = null;

            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()) {
                boolean isCommented = (matcher.group(1) != null);
                return new JVMOption(matcher.group(2)).setCommented(isCommented).setValue(matcher.group(3));
            }

        return result;
    }
}
