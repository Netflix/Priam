/*
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam.cassandra.extensions;

import java.lang.instrument.Instrumentation;
import java.util.Iterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * A <a
 * href="http://docs.oracle.com/javase/6/docs/api/java/lang/instrument/package-summary.html">PreMain</a>
 * class to run inside of the cassandra process. Contacts Priam for essential cassandra startup
 * information like token and seeds.
 */
public class PriamStartupAgent {
    public static String REPLACED_ADDRESS_MIN_VER = "1.2.11";

    public static void premain(String agentArgs, Instrumentation inst) {
        PriamStartupAgent agent = new PriamStartupAgent();
        agent.setPriamProperties();
    }

    private void setPriamProperties() {
        String token = null;
        String seeds = null;
        boolean isReplace = false;
        String replacedIp = "";
        String extraEnvParams = null;

        while (true) {
            try {
                token =
                        DataFetcher.fetchData(
                                "http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_token");
                seeds =
                        DataFetcher.fetchData(
                                "http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_seeds");
                isReplace =
                        Boolean.parseBoolean(
                                DataFetcher.fetchData(
                                        "http://127.0.0.1:8080/Priam/REST/v1/cassconfig/is_replace_token"));
                replacedIp =
                        DataFetcher.fetchData(
                                "http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_replaced_ip");
                extraEnvParams =
                        DataFetcher.fetchData(
                                "http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_extra_env_params");

            } catch (Exception e) {
                System.out.println(
                        "Failed to obtain startup data from priam, can not start yet. will retry shortly");
                e.printStackTrace();
            }

            if (token != null && seeds != null) break;
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e1) {
                // do nothing.
            }
        }

        System.setProperty("cassandra.initial_token", token);

        setExtraEnvParams(extraEnvParams);

        if (isReplace) {
            System.out.println(
                    "Detect cassandra version : " + FBUtilities.getReleaseVersionString());
            if (FBUtilities.getReleaseVersionString().compareTo(REPLACED_ADDRESS_MIN_VER) < 0) {
                System.setProperty("cassandra.replace_token", token);
            } else {
                System.setProperty("cassandra.replace_address_first_boot", replacedIp);
            }
        }
    }

    private void setExtraEnvParams(String extraEnvParams) {
        try {
            if (null != extraEnvParams && extraEnvParams.length() > 0) {
                JSONParser parser = new JSONParser();
                Object obj = parser.parse(extraEnvParams);
                JSONObject jsonObj = (JSONObject) obj;
                if (jsonObj.size() > 0) {
                    for (Iterator iterator = jsonObj.keySet().iterator(); iterator.hasNext(); ) {
                        String key = (String) iterator.next();
                        String val = (String) jsonObj.get(key);
                        if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(val)) {
                            System.setProperty(key.trim(), val.trim());
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(
                    "Failed to parse extra env params: "
                            + extraEnvParams
                            + ". However, ignoring the exception.");
            e.printStackTrace();
        }
    }
}
