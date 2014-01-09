package net.nationalfibre.amqphdfs;

import com.rabbitmq.client.Connection;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static net.nationalfibre.amqphdfs.ShovelConfig.create;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static org.apache.hadoop.fs.FileSystem.SHUTDOWN_HOOK_PRIORITY;
import org.apache.hadoop.util.ShutdownHookManager;


public class Main
{
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final ExecutorService executor = Executors.newFixedThreadPool(1);
    private static final Log logger = LogFactory.getLog(Main.class);


    public static void main (String[] args) throws IOException
    {
        if (args.length < 1) {
            System.err.println("Invalid config file");
            logger.fatal("Invalid config file");
            System.exit(-1);

            return;
        }

        final Config rootConfig                 = ConfigFactory.parseFile(new File(args[0]));
        final Config shovelConfig               = rootConfig.getConfig("amqp-to-hdfs-shovel");
        final Map<String, ShovelConfig> configs = ShovelConfig.createAll(shovelConfig);
        final Connection conn                   = ShovelConfig.createAmqpConnection(executor, shovelConfig);
        final ShovelManager manager             = new ShovelManager(conn, scheduler);
        final Runnable shutdownHook             = new Runnable() {
            @Override
            public void run() {
                logger.info("Stoping ...");
                manager.stop();
                logger.info("Done ...");
            }
        };

        logger.info("Starting ...");

        ShutdownHookManager.get().addShutdownHook(shutdownHook, SHUTDOWN_HOOK_PRIORITY + 1);

        for (ShovelConfig config : configs.values()) {
            manager.start(config);
        }

        logger.info("Started ...");
    }
}
