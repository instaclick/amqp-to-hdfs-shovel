package net.nationalfibre.amqphdfs;

import com.rabbitmq.client.Connection;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;
import org.apache.hadoop.conf.Configuration;

public class ShovelConfig
{
    final String queueName;
    Configuration hdfsConf;
    String filePrefix;
    String hdfsHost;
    String hdfsPath;
    Long windowsSize;
    Mode mode;

    private ShovelConfig(String queueName)
    {
        this.queueName = queueName;
    }

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

    public ShovelConfig withMode(Mode mode)
    {
        this.mode = mode;

        return this;
    }

    public Mode getMode()
    {
        return mode;
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

    public String getHdfsPath()
    {
        return hdfsPath;
    }

    public ShovelConfig withHdfsPath(String path)
    {
        if (path != null && path.endsWith("/")) {
            path = path.substring(0, path.length()-1);
        }

        this.hdfsPath = path;

        return this;
    }

    public String getQueueName()
    {
        return queueName;
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

    public long getCurrentWindow()
    {
        return (System.currentTimeMillis() / 1000) / this.windowsSize;
    }

    public String getGenerateUnique()
    {
        if (mode == Mode.LONG) {
            return String.valueOf(Math.abs(UUID.randomUUID().getMostSignificantBits()));
        }

        if (mode == Mode.UUID) {
            return UUID.randomUUID().toString();
        }

        return String.valueOf(System.currentTimeMillis());
    }

    public String getTmpFileName(String name)
    {
        return getFileName(name + ".tmp");
    }

    public String getFileName(String name)
    {
        String prefix = filePrefix != null ? filePrefix : "";

        return hdfsPath + "/" + prefix + name;
    }

    public static ShovelConfig create(String name)
    {
        return new ShovelConfig(name);
    }

    public static Map<String, ShovelConfig> createAll(final Config config)
    {
        final Map<String, ShovelConfig> map = new HashMap<String, ShovelConfig>();
        final Configuration hdfsConfig = new Configuration();
        final Mode mode = config.hasPath("rotate.mode")
            ? Mode.valueOf(config.getString("rotate.mode"))
            : Mode.UUID;

        hdfsConfig.set("fs.defaultFS", config.getString("hdfs.host"));

        for (Config current : config.getConfigList("shovels")) {
            final String name      = current.getString("name");
            final ShovelConfig cfg = create(name);

            cfg.withHdfsHost(config.getString("hdfs.host"))
                .withHdfsConf(hdfsConfig)
                .withMode(mode);

            cfg.withWindowsSize(current.getLong("window"))
                .withHdfsPath(current.getString("path"));

            if (current.hasPath("prefix")) {
                cfg.withFilePrefix(current.getString("prefix"));
            }

            map.put(name, cfg);
        }

        return map;
    }

    public static Connection createAmqpConnection(final ExecutorService executor, Config config) throws IOException
    {
        final ConnectionOptions options = new ConnectionOptions()
                .withConsumerExecutor(executor)
                .withPort(config.getInt("amqp.port"))
                .withHost(config.getString("amqp.host"))
                .withUsername(config.getString("amqp.user"))
                .withPassword(config.getString("amqp.pass"))
                .withVirtualHost(config.getString("amqp.vhost"));

        final net.jodah.lyra.config.Config c = new net.jodah.lyra.config.Config()
            .withRecoveryPolicy(RecoveryPolicies.recoverAlways())
            .withRetryPolicy(new RetryPolicy().withBackoff(Duration.seconds(1), Duration.seconds(30)));

        return Connections.create(options, c);
    }

    enum Mode
    {
        LONG,
        UUID,
        TIME
    }
}
