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

package com.netflix.priam.notification;

/**
 * Created by aagrawal on 8/11/17.
 */
public interface EventGenerator<T> {
    void addObserver(EventObserver<T> observer);
    void removeObserver(EventObserver<T> observer);
    void notifyEventStart(T event);
    void notifyEventSuccess(T event);
    void notifyEventFailure(T event);
    void notifyEventStop(T event);
}
