package com.priam.defaultimpl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.google.inject.Inject;
import com.priam.aws.ICredential;

public class PropertyCredential implements ICredential
{

    private Properties prop;

    @Inject
    public PropertyCredential() throws IOException
    {
        this.prop = new Properties();
        FileInputStream is = new FileInputStream("conf/awscredentials.properties");
        prop.load(is);
    }

    @Override
    public String getAccessKeyId()
    {
        return prop.getProperty("AccessKey");
    }

    @Override
    public String getSecretAccessKey()
    {
        return prop.getProperty("SecretAccessKey");
    }

}
