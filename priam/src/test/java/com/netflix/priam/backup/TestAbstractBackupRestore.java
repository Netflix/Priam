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

package com.netflix.priam.backup;

import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.backup.AbstractBackupRestore.DIRECTORYTYPE;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by aagrawal on 8/15/17.
 */
public class TestAbstractBackupRestore {

    @Test
    public void testFilter()
    {
        MyAbstractBackupRestore abstractBackupRestore = new MyAbstractBackupRestore();

        Assert.assertTrue(abstractBackupRestore.resetFilters("abc", null).isFiltered(DIRECTORYTYPE.KEYSPACE, "abc"));
        Assert.assertTrue(abstractBackupRestore.resetFilters("ab.*", null).isFiltered(DIRECTORYTYPE.KEYSPACE, "abc"));

        Assert.assertTrue(abstractBackupRestore.resetFilters("abc,def", null).isFiltered(DIRECTORYTYPE.KEYSPACE, "abc"));
        Assert.assertTrue(abstractBackupRestore.resetFilters("abc,def", null).isFiltered(DIRECTORYTYPE.KEYSPACE, "def"));
        Assert.assertTrue(abstractBackupRestore.resetFilters("ab.*,de.*", null).isFiltered(DIRECTORYTYPE.KEYSPACE, "abc"));
        Assert.assertTrue(abstractBackupRestore.resetFilters("ab.*,de.*", null).isFiltered(DIRECTORYTYPE.KEYSPACE, "def"));

        Assert.assertFalse(abstractBackupRestore.resetFilters(null, null).isFiltered(DIRECTORYTYPE.KEYSPACE, "ab"));
        Assert.assertFalse(abstractBackupRestore.resetFilters("abc", null).isFiltered(DIRECTORYTYPE.KEYSPACE, "ab"));
        Assert.assertFalse(abstractBackupRestore.resetFilters("abc", null).isFiltered(DIRECTORYTYPE.KEYSPACE, "cde"));

        Assert.assertTrue(abstractBackupRestore.resetFilters(null, "abc.def").isFiltered(DIRECTORYTYPE.CF, "abc", "def"));
        Assert.assertTrue(abstractBackupRestore.resetFilters(null, "abc.de.*").isFiltered(DIRECTORYTYPE.CF, "abc", "def"));
        Assert.assertTrue(abstractBackupRestore.resetFilters(null, "abc.def,ab.cd.*").isFiltered(DIRECTORYTYPE.CF, "ab", "cd"));
        Assert.assertTrue(abstractBackupRestore.resetFilters(null, "abc.de.*,ab.cd.*").isFiltered(DIRECTORYTYPE.CF, "abc", "def"));

        Assert.assertFalse(abstractBackupRestore.resetFilters(null, null).isFiltered(DIRECTORYTYPE.CF, "ab"));
        Assert.assertFalse(abstractBackupRestore.resetFilters(null, null).isFiltered(DIRECTORYTYPE.CF, "ab", "cd"));
        Assert.assertFalse(abstractBackupRestore.resetFilters("ab", null).isFiltered(DIRECTORYTYPE.CF, "ab", "cd"));
    }

    private class MyAbstractBackupRestore extends AbstractBackupRestore
    {
        private String keyspaceFilter;
        private String columnfamilyFilter;

        MyAbstractBackupRestore()
        {
            super(new FakeConfiguration());
        }

        MyAbstractBackupRestore resetFilters(String keyspaceFilter, String columnfamilyFilter)
        {
            this.keyspaceFilter = keyspaceFilter;
            this.columnfamilyFilter = columnfamilyFilter;
            populateFilters();
            return this;
        }

        @Override
        protected String getConfigKeyspaceFilter() {
            return keyspaceFilter;
        }

        @Override
        protected String getConfigColumnfamilyFilter() {
            return columnfamilyFilter;
        }

        @Override
        public void execute() throws Exception {

        }

        @Override
        public String getName() {
            return "MyAbstractBackupRestore";
        }
    }
}
