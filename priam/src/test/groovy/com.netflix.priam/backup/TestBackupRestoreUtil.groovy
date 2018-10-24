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
