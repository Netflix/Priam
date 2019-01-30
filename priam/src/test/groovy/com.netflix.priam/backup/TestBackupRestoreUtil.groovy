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
package com.netflix.priam.backup

import spock.lang.Specification
import spock.lang.Unroll

/**
 Created by aagrawal on 8/15/17.
 */
@Unroll
class TestBackupRestoreUtil extends Specification {
    def "IsFilter for KS #keyspace and CF #columnfamily with configuration include #configIncludeFilter and exclude #configExcludeFilter is #result"() {
        expect:
        new BackupRestoreUtil(configIncludeFilter, configExcludeFilter).isFiltered(keyspace, columnfamily) == result

        where:
        configIncludeFilter | configExcludeFilter | keyspace | columnfamily || result
        null                | null                | "defg"   | "gh"         || false
        "abc.*"             | null                | "abc"    | "cd"         || false
        "abc.*"             | null                | "ab"     | "cd"         || true
        null                | "abc.de"            | "abc"    | "def"        || false
        null                | "abc.de"            | "abc"    | "de"         || true
        "abc.*,def.*"       | null                | "abc"    | "cd"         || false
        "abc.*,def.*"       | null                | "def"    | "ab"         || false
        "abc.*,def.*"       | null                | "ab"     | "cd"         || true
        "abc.*,def.*"       | null                | "df"     | "ab"         || true
        null                | "abc.de,fg.hi"      | "abc"    | "def"        || false
        null                | "abc.de,fg.hi"      | "abc"    | "de"         || true
        null                | "abc.de,fg.hi"      | "fg"     | "hijk"       || false
        null                | "abc.de,fg.hi"      | "fg"     | "hi"         || true
        "abc.*"             | "ab.ab"             | "ab"     | "cd"         || true
        "abc.*"             | "ab.ab"             | "ab"     | "ab"         || true
        "abc.*"             | "abc.ab"            | "abc"    | "ab"         || true
        "abc.*"             | "abc.ab"            | "abc"    | "cd"         || false
        "abc.cd"            | "abc.*"             | "abc"    | "cd"         || true
        "abc.*"             | "abc.*"             | "abc"    | "cd"         || true
        "abc.*,def.*"       | "abc.*"             | "def"    | "ab"         || false
    }


    def "Expected exception KS #keyspace and CF #columnfamily with configuration include #configIncludeFilter and exclude #configExcludeFilter"() {
        when:
        new BackupRestoreUtil(configIncludeFilter, configExcludeFilter).isFiltered(keyspace, columnfamily)

        then:
        thrown(ExcpectedException)

        where:
        configIncludeFilter | configExcludeFilter | keyspace | columnfamily || ExcpectedException
        null                | "def"               | "defg"   | null         || IllegalArgumentException
        "abc"               | null                | null     | "cd"         || IllegalArgumentException
    }
}
