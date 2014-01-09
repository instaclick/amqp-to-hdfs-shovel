package net.nationalfibre.amqphdfs;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Shovel
{
    final Map<Long, BufferedWriter> buffers = new ConcurrentHashMap<Long, BufferedWriter>();
    final AtomicReference<Long> tagReference = new AtomicReference(-1L);
    final Log logger = LogFactory.getLog(Shovel.class);
    final ShovelConfig conf;
    final Channel channel;

    Long lastTagReference = -1L;

    public Shovel(Channel channel, ShovelConfig conf)
    {
        this.channel    = channel;
        this.conf       = conf;
    }

    protected FileSystem getFileSystem() throws IOException
    {
        return FileSystem.get(conf.getHdfsConf());
    }

    public Consumer createConsumer() throws IOException
    {
        return new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
            {
                final Long timeWindow       = conf.getCurrentTime();
                final String filePath       = conf.getTmpFileName(timeWindow);
                final Path path             = new Path(filePath);

                try {
                    final BufferedWriter writer = ( ! buffers.containsKey(timeWindow))
                            ? new BufferedWriter(new OutputStreamWriter(getFileSystem().create(path, true)))
                            : buffers.get(timeWindow);

                    writer.append(new String(body) + "\n");
                    writer.flush();

                    if (envelope.getDeliveryTag() > tagReference.get()) {
                        tagReference.set(envelope.getDeliveryTag());
                    }

                    if ( ! buffers.containsKey(timeWindow)) {
                        logger.debug("Create tmp file : " + filePath);
                        buffers.put(timeWindow, writer);
                    }

                } catch (IOException ex) {

                    logger.error(this, ex);
                    buffers.clear();

                    try {
                        FileSystem.closeAll();
                    } catch (IOException ex1) {
                        logger.error(this, ex1);
                    }

                    try {
                        channel.basicReject(envelope.getDeliveryTag(), true);
                    } catch (IOException ex1) {
                        logger.error(this, ex1);
                    }

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex1) {
                        logger.error(this, ex1);
                    }
                } catch (Exception ex) {
                    logger.error(this, ex);
                }
            }
        };
    }

    public void consume(Consumer consumer) throws IOException
    {
        logger.debug("Starting consumer");
        channel.basicConsume(conf.getQueueName(), consumer);
        logger.debug("basic consumer started");
    }

    public void consume() throws IOException
    {
        consume(createConsumer());
    }

    public synchronized void rotate() throws IOException
    {
        final Long currentTimeWindow  = conf.getCurrentTime();
        final Set<Long> bufferKeys    = buffers.keySet();

        if (getFileSystem() == null) {
            logger.debug("Ignore hdfs connection failure");
            return;
        }

        if (channel.getConnection() == null || ! channel.getConnection().isOpen()) {
            logger.debug("Ignore amqp connection failure");
            return;
        }

        for (Long key : bufferKeys) {

            if (currentTimeWindow.equals(key)) {
                continue;
            }

            try {
                buffers.get(key).close();
            } catch (IOException e) {
                logger.error(this, e);
            }

            buffers.remove(key);

            final Path fromPath = new Path(conf.getTmpFileName(key));
            final Path toPath   = new Path(conf.getFileName(String.valueOf(key)));

            if ( ! getFileSystem().exists(fromPath)) {
                logger.debug("Ignore file : " + fromPath.getName());
                continue;
            }

            getFileSystem().rename(fromPath, toPath);
            logger.debug(String.format("rotate ('%s','%s'): ", fromPath.getName(), toPath.getName()));
        }

        if (tagReference.get() > lastTagReference) {
            lastTagReference = tagReference.get();

            logger.debug(String.format("%s - Message Ack", lastTagReference));

            channel.basicAck(lastTagReference, true);
        }
    }
}
