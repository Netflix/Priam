/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.priam.notification;

/**
 * Subscriber who wishes to receive event notifications from the {@link EventGenerator} Created by
 * aagrawal on 8/11/17.
 */
public interface EventObserver<T> {
    /**
     * Notification of an event start from the {@link EventGenerator}
     *
     * @param event Event for which notification was sent.
     */
    void updateEventStart(T event);

    /**
     * Notification of an event failure from the {@link EventGenerator}
     *
     * @param event Event for which notification was sent.
     */
    void updateEventFailure(T event);

    /**
     * Notification of an event success from the {@link EventGenerator}
     *
     * @param event Event for which notification was sent.
     */
    void updateEventSuccess(T event);

    /**
     * Notification of an event stop from the {@link EventGenerator}
     *
     * @param event Event for which notification was sent.
     */
    void updateEventStop(T event);
}
