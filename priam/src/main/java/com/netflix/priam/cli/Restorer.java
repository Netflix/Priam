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
package com.netflix.priam.cli;

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.restore.Restore;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Restorer {
    private static final Logger logger = LoggerFactory.getLogger(Restorer.class);

    static void displayHelp() {
        System.out.println("Usage: command_name FROM_DATE TO_DATE");
    }

    public static void main(String[] args) {
        try {
            Application.initialize();

            Date startTime, endTime;
            if (args.length < 2) {
                displayHelp();
                return;
            }
            AbstractBackupPath path =
                    Application.getInjector().getInstance(AbstractBackupPath.class);
            startTime = path.parseDate(args[0]);
            endTime = path.parseDate(args[1]);

            Restore restorer = Application.getInjector().getInstance(Restore.class);
            try {
                restorer.restore(startTime, endTime);
            } catch (Exception e) {
                logger.error("Unable to restore: ", e);
            }
        } finally {
            Application.shutdownAdditionalThreads();
        }
    }
}
