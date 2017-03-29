package com.netflix.priam.dse;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import com.google.common.io.Files;
import com.netflix.priam.FakeConfiguration;
import com.netflix.priam.IConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class DseTunerTest
{
    IConfiguration config;
    DseConfigStub dseConfig;
    DseTuner dseTuner;
    File targetFile;
    File targetDseYamlFile;

    @Before
    public void setup() throws IOException
    {
        config = new FakeConfiguration();
        dseConfig = new DseConfigStub();
        dseTuner = new DseTuner(config, dseConfig);

        File targetDir = new File(config.getCassHome() + "/conf");
        if(!targetDir.exists())
            targetDir.mkdirs();

        targetFile = new File(config.getCassHome() + DseTuner.AUDIT_LOG_FILE);
        Files.copy(new File("src/test/resources/" + DseTuner.AUDIT_LOG_FILE), targetFile);
    }

    @Test
    public void auditLogProperties_Enabled() throws IOException
    {
        dseConfig.setAuditLogEnabled(true);
        dseTuner.writeAuditLogProperties();

        Properties p = new Properties();
        p.load(new FileReader(targetFile));
        Assert.assertTrue(p.containsKey(DseTuner.PRIMARY_AUDIT_LOG_ENTRY));
    }

    @Test
    public void auditLogProperties_Disabled() throws IOException
    {
        dseConfig.setAuditLogEnabled(false);
        dseTuner.writeAuditLogProperties();

        Properties p = new Properties();
        p.load(new FileReader(targetFile));
        Assert.assertFalse(p.containsKey(DseTuner.PRIMARY_AUDIT_LOG_ENTRY));
    }

    /**
     * This is different because we test the disabled step using the already used enabled file
     * (not a clean copy over of the original props file from the resources dir), and vice versa
     *
     * @throws IOException
     */
    @Test
    public void auditLogProperties_ThereAndBackAgain() throws IOException
    {
        auditLogProperties_Enabled();
        auditLogProperties_Disabled();
        auditLogProperties_Enabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Enabled();
        auditLogProperties_Enabled();
        auditLogProperties_Enabled();
        auditLogProperties_Enabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Disabled();
        auditLogProperties_Enabled();
        auditLogProperties_Enabled();
    }

    @Test
    public void auditLogYamlProperties_Enabled() throws IOException {
        File targetDseDir = new File(config.getCassHome() + "/resources/dse/conf/");
        if(!targetDseDir.exists()) {
            targetDseDir.mkdirs();
        }

        int index = dseConfig.getDseYamlLocation().lastIndexOf('/') + 1;
        targetDseYamlFile = new File(targetDseDir + dseConfig.getDseYamlLocation().substring(index - 1));
        Files.copy(new File("src/test/resources/conf/" + dseConfig.getDseYamlLocation().substring(index)), targetDseYamlFile);


        dseConfig.setAuditLogEnabled(true);
        dseTuner.writeDseYaml();

    }

    @Test
    public void auditLogYamlProperties_Disabled() throws IOException {
        File targetDseDir = new File(config.getCassHome() + "/resources/dse/conf/");
        if(!targetDseDir.exists()) {
            targetDseDir.mkdirs();
        }

        int index = dseConfig.getDseYamlLocation().lastIndexOf('/') + 1;
        targetDseYamlFile = new File(targetDseDir + dseConfig.getDseYamlLocation().substring(index - 1));
        Files.copy(new File("src/test/resources/conf/" + dseConfig.getDseYamlLocation().substring(index)), targetDseYamlFile);


        dseConfig.setAuditLogEnabled(false);
        dseTuner.writeDseYaml();

    }
}
