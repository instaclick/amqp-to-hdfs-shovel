package net.nationalfibre.amqphdfs;

import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ShovelManager
{
    final Connection conn;
    final ScheduledExecutorService scheduler;
    final Log logger = LogFactory.getLog(ShovelManager.class);
    final Map<String, Shovel> map = new HashMap<String, Shovel>();

    public ShovelManager(Connection conn, ScheduledExecutorService scheduler)
    {
        this.conn      = conn;
        this.scheduler = scheduler;
    }

    public void start(ShovelConfig shovelConfig) throws IOException
    {
        final Shovel  shovel   = new Shovel(conn.createChannel(), shovelConfig);
        final Runnable rotator = createRotator(shovel);

        scheduler.scheduleAtFixedRate(rotator, shovelConfig.getWindowsSize(), shovelConfig.getWindowsSize(), TimeUnit.SECONDS);
        map.put(shovelConfig.getQueueName(), shovel);

        shovel.consume();
    }

    protected Runnable createRotator(final Shovel  shovel)
    {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    shovel.rotate();
                } catch (IOException ex) {
                    logger.error(this, ex);
                }
            }
        };
    }

    public void stop()
    {
        try {
            for (Map.Entry<String, Shovel> entry : map.entrySet()) {
                final String name   = entry.getKey();
                final Shovel shovel = entry.getValue();

                logger.info("Stoping shovel : " + name);
                shovel.flush(-1L);

                logger.info("Closing amqp channel");
                shovel.getChannel().close();
            }
        } catch (IOException e) {
            logger.warn(e);
        }

        try {
            conn.close();
        } catch (IOException e) {
            logger.warn(e);
        }
    }
}
