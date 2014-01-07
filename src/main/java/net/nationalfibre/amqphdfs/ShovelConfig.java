package net.nationalfibre.amqphdfs;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;

public class ShovelConfig
{
    Configuration hdfsConf;
    String filePrefix;
    String hdfsHost;
    String rootPath;
    String queueName;
    String amqpHost;
    String amqpVHost = "/";
    String amqpUsername;
    String amqpPassword;
    int amqpQos = 0;
    Long windowsSize;
    
    public ShovelConfig withWindowsSize(Long windowsSize)
    {
        this.windowsSize = windowsSize;

        return this;
    }

    public ShovelConfig withFilePrefix(String prefix)
    {
        this.filePrefix = prefix;

        return this;
    }

    public Long getWindowsSize()
    {
        return windowsSize;
    }

    public String getFilePrefix()
    {
        return filePrefix;
    }

    public String getHdfsHost()
    {
        return hdfsHost;
    }

    public ShovelConfig withHdfsHost(String hdfsHost)
    {
        this.hdfsHost = hdfsHost;

        return this;
    }

    public String getRootPath()
    {
        return rootPath;
    }

    public ShovelConfig withRootPath(String rootPath)
    {
        if (rootPath != null && rootPath.endsWith("/")) {
            rootPath = rootPath.substring(0, rootPath.length()-1);
        }

        this.rootPath = rootPath;

        return this;
    }

    public String getQueueName()
    {
        return queueName;
    }

    public ShovelConfig withQueueName(String queueName)
    {
        this.queueName = queueName;

        return this;
    }

    public String getAmqpHost()
    {
        return amqpHost;
    }

    public ShovelConfig withAmqpHost(String amqpHost)
    {
        this.amqpHost = amqpHost;

        return this;
    }

    public String getAmqpVHost()
    {
        return amqpVHost;
    }

    public ShovelConfig withAmqpVHost(String amqpVHost)
    {
        this.amqpVHost = amqpVHost;

        return this;
    }

    public String getAmqpUsername()
    {
        return amqpUsername;
    }

    public ShovelConfig withAmqpUsername(String amqpUsername)
    {
        this.amqpUsername = amqpUsername;

        return this;
    }

    public String getAmqpPassword()
    {
        return amqpPassword;
    }

    public ShovelConfig withAmqpPassword(String amqpPassword)
    {
        this.amqpPassword = amqpPassword;

        return this;
    }

    public int getAmqpQos()
    {
        return amqpQos;
    }

    public ShovelConfig withAmqpQos(int amqpQos)
    {
        this.amqpQos = amqpQos;

        return this;
    }

    public Configuration getHdfsConf()
    {
        return hdfsConf;
    }

    public ShovelConfig withHdfsConf(Configuration hdfsConf)
    {
        this.hdfsConf = hdfsConf;

        return this;
    }

    public long getCurrentTime()
    {
        return (System.currentTimeMillis() / 1000) / this.windowsSize;
    }

    public String getTmpPathName()
    {
        return getTmpFileName(getCurrentTime());
    }

    public String getTmpFileName(Long timeWindow)
    {
        return getTmpFileName(String.valueOf(timeWindow));
    }

    public String getTmpFileName(String name)
    {
        return getFileName(name + ".tmp");
    }

    public String getFileName(String name)
    {
        String prefix = filePrefix != null ? filePrefix : "";

        return rootPath + "/" + prefix + name;
    }

    public static ShovelConfig create()
    {
        return new ShovelConfig();
    }

    public static ShovelConfig create(Config config)
    {
        Configuration hdfsConfig = new Configuration();
        ShovelConfig self        = create();

        hdfsConfig.set("fs.defaultFS", config.getString("amqp-to-hdfs-shovel.hdfs.host"));

        self.withWindowsSize(config.getLong("amqp-to-hdfs-shovel.time.window"))
            .withHdfsHost(config.getString("amqp-to-hdfs-shovel.hdfs.host"))
            .withRootPath(config.getString("amqp-to-hdfs-shovel.hdfs.path"))
            .withQueueName(config.getString("amqp-to-hdfs-shovel.queue.name"))
            .withFilePrefix(config.getString("amqp-to-hdfs-shovel.file.prefix"))
            .withAmqpHost(config.getString("amqp-to-hdfs-shovel.amqp.host"))
            .withAmqpUsername(config.getString("amqp-to-hdfs-shovel.amqp.user"))
            .withAmqpPassword(config.getString("amqp-to-hdfs-shovel.amqp.pass"))
            .withAmqpQos(config.getInt("amqp-to-hdfs-shovel.amqp.qos"))
            .withHdfsConf(hdfsConfig);

        return self;
    }
}
