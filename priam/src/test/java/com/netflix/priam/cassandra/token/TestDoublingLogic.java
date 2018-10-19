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

package com.netflix.priam.cassandra.token;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

/** Seems like skip 3 is the magic number.... this test will make sure to test the same. */
public class TestDoublingLogic {
    private static final int RACS = 2;
    private static final int NODES_PER_RACS = 2;

    @Test
    public void testSkip() {
        List<String> nodes = new ArrayList<>();
        for (int i = 0; i < NODES_PER_RACS; i++)
            for (int j = 0; j < RACS; j++) nodes.add("RAC-" + j);
        // printNodes(nodes);

        List<String> newNodes = nodes;
        for (int i = 0; i < 15; i++) {
            int count = newNodes.size();
            newNodes = doubleNodes(newNodes);
            assertEquals(newNodes.size(), count * 2);
            // printNodes(newNodes);
            validate(newNodes, nodes);
        }
    }

    public void printNodes(List<String> nodes) {
        System.out.println("=== Printing - Array of Size :" + nodes.size());
        System.out.println(StringUtils.join(nodes, "\n"));
        System.out.println(
                "=====================Completed doubling==============================="
                        + nodes.size());
    }

    private void validate(List<String> newNodes, List<String> nodes) {
        String temp = "";
        int count = 0;
        for (String node : newNodes) {
            if (temp.equals(node)) count++;
            else count = 0;

            if (count == 2) {
                System.out.println("Found an issue.....");
                throw new RuntimeException();
            }
            temp = node;
        }

        // compare if they are the same set...
        boolean test = true;
        for (int i = 0; i < nodes.size(); i++) {
            if (!newNodes.get(i).equals(nodes.get(i))) test = false;
        }
        if (test)
            throw new RuntimeException(
                    "Awesome we are back to the natural order... No need to test more");
    }

    private List<String> doubleNodes(List<String> nodes) {
        List<String> lst = new ArrayList<>();
        Map<Integer, String> return_ = new HashMap<>();
        for (int i = 0; i < nodes.size(); i++) {
            return_.put(i * 2, nodes.get(i));
        }

        for (int i = 0; i < nodes.size() * 2; i++) {
            if (0 == i % 2) {

                // rotate
                if (i + 3 >= (nodes.size() * 2)) {
                    int delta = (i + 3) - (nodes.size() * 2);
                    return_.put(delta, return_.get(i));
                }
                return_.put(i + 3, return_.get(i));
            }
        }
        for (int i = 0; i < nodes.size() * 2; i++) lst.add(return_.get(i));

        return lst;
    }
}
