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
 * Generic interface which an event generator class should implement so all the observers could be
 * notified of the events. Any class interested in state change for the event can subscribe by
 * registering themselves. Created by aagrawal on 8/11/17.
 */
public interface EventGenerator<T> {
    /**
     * Subscribes {@code observer} to receive generated events.
     *
     * @param observer {@link EventObserver} interested in receiving updates from this event
     *     generator. May not be <code>null</code>.
     */
    void addObserver(EventObserver<T> observer);

    /**
     * Removes {@code observer} from receiving any further events from this generator.
     *
     * @param observer {@link EventObserver} that is to stop receiving updates from this event
     *     generator. May not be <code>null</code>.
     */
    void removeObserver(EventObserver<T> observer);

    /**
     * Notify all the observers of an event start who subscribed to receive events.
     *
     * @param event Generated event from the generator
     */
    void notifyEventStart(T event);

    /**
     * Notify all the observers of an event success who subscribed to receive events.
     *
     * @param event Event which was successful from the generator.
     */
    void notifyEventSuccess(T event);

    /**
     * Notify all the observers of an event failure who subscribed to receive events.
     *
     * @param event Event which was failure from the generator.
     */
    void notifyEventFailure(T event);

    /**
     * Notify all the observers of an event stop who subscribed to receive events.
     *
     * @param event Event which was stopped from the generator.
     */
    void notifyEventStop(T event);
}
