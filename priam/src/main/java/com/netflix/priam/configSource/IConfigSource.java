/*
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.priam.configSource;

import com.google.inject.ImplementedBy;
import java.util.List;

/** Defines the configurations for an application. */
@ImplementedBy(PriamConfigSource.class)
public interface IConfigSource {

    /**
     * Must be called before any other method. This method will allow implementations to do any
     * setup that they require before being called.
     *
     * @param asgName: Name of the asg
     * @param region: Name of the region
     */
    void initialize(String asgName, String region);

    /**
     * A non-negative integer indicating a count of elements.
     *
     * @return non-negative integer indicating a count of elements.
     */
    int size();

    /**
     * Returns {@code true} if the size is zero. May be more efficient than calculating size.
     *
     * @return {@code true} if the size is zero otherwise {@code false}.
     */
    boolean isEmpty();

    /**
     * Check if the given key can be found in the config.
     *
     * @param key to look up value.
     * @return if the key is present
     */
    boolean contains(String key);

    /**
     * Get a String associated with the given configuration key.
     *
     * @param key to look up value.
     * @return value from config or null if not present.
     */
    String get(String key);

    /**
     * Get a String associated with the given configuration key.
     *
     * @param key to look up value.
     * @param defaultValue if value is not present.
     * @return value from config or defaultValue if not present.
     */
    String get(String key, String defaultValue);

    /**
     * Get a boolean associated with the given configuration key.
     *
     * @param key to look up value.
     * @param defaultValue if value is not present.
     * @return value from config or defaultValue if not present.
     */
    boolean get(String key, boolean defaultValue);

    /**
     * Get a Class associated with the given configuration key.
     *
     * @param key to look up value.
     * @param defaultValue if value is not present.
     * @return value from config or defaultValue if not present.
     */
    Class<?> get(String key, Class<?> defaultValue);

    /**
     * Get a Enum associated with the given configuration key.
     *
     * @param key to look up value.
     * @param defaultValue if value is not present.
     * @param <T> enum type.
     * @return value from config or defaultValue if not present.
     */
    <T extends Enum<T>> T get(String key, T defaultValue);

    /**
     * Get a int associated with the given configuration key.
     *
     * @param key to look up value.
     * @param defaultValue if value is not present.
     * @return value from config or defaultValue if not present.
     */
    int get(String key, int defaultValue);

    /**
     * Get a long associated with the given configuration key.
     *
     * @param key to look up value.
     * @param defaultValue if value is not present.
     * @return value from config or defaultValue if not present.
     */
    long get(String key, long defaultValue);

    /**
     * Get a float associated with the given configuration key.
     *
     * @param key to look up value.
     * @param defaultValue if value is not present.
     * @return value from config or defaultValue if not present.
     */
    float get(String key, float defaultValue);

    /**
     * Get a double associated with the given configuration key.
     *
     * @param key to look up value.
     * @param defaultValue if value is not present.
     * @return value from config or defaultValue if not present.
     */
    double get(String key, double defaultValue);

    /**
     * Get a list of strings associated with the given configuration key.
     *
     * @param key to look up value.
     * @return value from config or an immutable list if not present.
     */
    List<String> getList(String key);

    /**
     * Get a list of strings associated with the given configuration key.
     *
     * @param key to look up value.
     * @param defaultValue if value is not present.
     * @return value from config or defaultValue if not present.
     */
    List<String> getList(String key, List<String> defaultValue);

    /**
     * Set the value for the given key.
     *
     * @param key to set value for.
     * @param value to set.
     */
    void set(String key, String value);
}
