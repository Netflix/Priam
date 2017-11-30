package com.netflix.priam.backup

import spock.lang.Specification
import spock.lang.Unroll

/**
 Created by aagrawal on 8/15/17.
 */
@Unroll
class TestBackupRestoreUtil extends Specification {
    def "IsFilter for KS #keyspace with configuration #configKeyspaceFilter is #result"() {
        expect:
        new BackupRestoreUtil(configKeyspaceFilter, configCFFilter).isFiltered(BackupRestoreUtil.DIRECTORYTYPE.KEYSPACE, keyspace, columnfamily) == result

        where:
        configKeyspaceFilter | configCFFilter | keyspace | columnfamily || result
        "abc"                | null           | "abc"    | null         || true
        "abc"                | "ab.ab"        | "ab"     | null         || false
        "abc"                | null           | "ab"     | null         || false
        "abc,def"            | null           | "abc"    | null         || true
        "abc,def"            | null           | "def"    | null         || true
        "abc,def"            | null           | "ab"     | null         || false
        "abc,def"            | null           | "df"     | null         || false
        "ab.*"               | null           | "ab"     | null         || true
        "ab.*,def"           | null           | "ab"     | null         || true
        "ab.*,de.*"          | null           | "ab"     | null         || true
        "ab.*,de.*"          | null           | "abab"   | null         || true
        "ab.*,de.*"          | null           | "defg"   | null         || true
        null                 | null           | "defg"   | null         || false
    }

    def "IsFilter for CF #columnfamily with configuration #configCFFilter is #result"() {
        expect:
        new BackupRestoreUtil(configKeyspaceFilter, configCFFilter).isFiltered(BackupRestoreUtil.DIRECTORYTYPE.CF, keyspace, columnfamily) == result

        where:
        configKeyspaceFilter | configCFFilter     | keyspace | columnfamily || result
        "abc"                | null               | "abc"    | null         || false
        "abc"                | "ab.ab"            | "ks"     | "ab"         || false
        "abc"                | "ab.ab"            | "ab"     | "ab.ab"      || true
        "abc"                | "ab.ab,de.fg"      | "ab"     | "ab.ab"      || true
        "abc"                | "ab.ab,de.fg"      | "de"     | "fg"         || true
        null                 | "abc.de.*"         | "abc"    | "def"        || true
        null                 | "abc.de.*"         | "abc"    | "abc.def"    || true
        null                 | "abc.de.*,fg.hi.*" | "abc"    | "def"        || true
        null                 | "abc.de.*,fg.hi.*" | "abc"    | "abc.def"    || true
        null                 | "abc.de.*,fg.hi.*" | "fg"     | "hijk"       || true
        null                 | "abc.de.*,fg.hi.*" | "fg"     | "fg.hijk"    || true

    }
}
