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
package com.netflix.priam.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * Created by aagrawal on 10/12/17.
 */
public class GsonJsonSerializer {
    private static final Gson gson = new GsonBuilder()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
            .setPrettyPrinting()
            .disableHtmlEscaping()
            .registerTypeAdapter(Date.class, new DateTypeAdapter())
            .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeTypeAdapter())
            .create();

    public static Gson getGson(){
        return gson;
    }

    static class DateTypeAdapter extends TypeAdapter<Date> {
        @Override
        public void write(JsonWriter out, Date value)
                throws IOException {
            out.value(DateUtil.formatyyyyMMddHHmm(value));
        }

        @Override
        public Date read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }
            String result = in.nextString();
            if ("".equals(result)) {
                return null;
            }
            return DateUtil.getDate(result);

        }
    }

    static class LocalDateTimeTypeAdapter extends TypeAdapter<LocalDateTime> {
        @Override
        public void write(JsonWriter out, LocalDateTime value)
                throws IOException {
            out.value(DateUtil.formatyyyyMMddHHmm(value));
        }

        @Override
        public LocalDateTime read(JsonReader in) throws IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }
            String result = in.nextString();
            if ("".equals(result)) {
                return null;
            }
            return DateUtil.getLocalDateTime(result);

        }
    }
}
