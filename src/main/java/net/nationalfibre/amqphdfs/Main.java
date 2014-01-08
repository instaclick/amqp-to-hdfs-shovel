package net.nationalfibre.amqphdfs;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicies;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

public class Main
{
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final ExecutorService executor = Executors.newFixedThreadPool(1);
    private static final Log logger = LogFactory.getLog(Main.class);
    private static final Configuration conf = new Configuration();

    public static void main (String[] args) throws IOException
    {
        if (args.length < 1) {
            System.err.println("Invalid config file");
            System.exit(-1);
            return;
        }

        final ShovelConfig shovelConfig = ShovelConfig.create(ConfigFactory.parseFile(new File(args[0])));

        conf.set("fs.defaultFS", shovelConfig.getHdfsHost());

        final ConnectionOptions options = new ConnectionOptions()
                .withConsumerExecutor(executor)
                .withHost(shovelConfig.getAmqpHost())
                .withPort(shovelConfig.getAmqpPort())
                .withUsername(shovelConfig.getAmqpUsername())
                .withPassword(shovelConfig.getAmqpPassword())
                .withVirtualHost(shovelConfig.getAmqpVHost());

        final Config config = new Config()
            .withRecoveryPolicy(RecoveryPolicies.recoverAlways())
            .withRetryPolicy(new RetryPolicy().withBackoff(Duration.seconds(1), Duration.seconds(30)));

        final Connection connection = Connections.create(options, config);
        final Channel channel       = connection.createChannel();
        final Shovel  shovel        = new Shovel(channel, shovelConfig);
        final Runnable rotator      = new Runnable() {
            @Override
            public void run() {
                try {
                    shovel.rotate();
                } catch (IOException ex) {
                    logger.error(this, ex);
                }
            }
        };

        logger.debug("Start ...");

        channel.basicQos(shovelConfig.getAmqpQos());
        start(channel, shovel);

        scheduler.scheduleAtFixedRate(rotator, shovelConfig.getWindowsSize(), shovelConfig.getWindowsSize(), TimeUnit.SECONDS);
    }

    public static void start(final Channel channel, final Shovel shovel)
    {
        try {
            shovel.consume();
        } catch (Exception e) {
            logger.warn(e);
            try {
                Thread.sleep(1);
                start(channel, shovel);
            } catch (InterruptedException ex) {
                logger.error(ex);
            }
        }
    }
}
