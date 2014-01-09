package net.nationalfibre.amqphdfs;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import java.io.BufferedWriter;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ShovelTest
{
    @Test
    public void testRotateShouldCloseTmpFiles() throws Exception
    {
        final Channel channel     = mock(Channel.class);
        final FileSystem fs       = mock(FileSystem.class);
        final ShovelConfig cfg    = mock(ShovelConfig.class);
        final BufferedWriter b1   = mock(BufferedWriter.class);
        final BufferedWriter b2   = mock(BufferedWriter.class);
        final Shovel instance     = new Shovel(channel, cfg){
            @Override
            protected FileSystem getFileSystem() throws IOException
            {
                return fs;
            }
        };

        instance.buffers.put(1L, b1);
        instance.buffers.put(2L, b2);
        instance.tagReference.set(11L);

        when(cfg.getCurrentTime()).thenReturn(2L);
        when(cfg.getTmpFileName(1L)).thenReturn("/tmp/1.tmp");
        when(cfg.getFileName("1")).thenReturn("/tmp/1");

        when(fs.exists(new Path("/tmp/1.tmp"))).thenReturn(true);
        when(fs.rename(new Path("/tmp/1.tmp"), new Path("/tmp/1"))).thenReturn(true);

        instance.rotate();

        verify(b1).close();
        verify(b2, times(0)).close();
        verify(fs).rename(new Path("/tmp/1.tmp"), new Path("/tmp/1"));
        verify(channel).basicAck(11L, true);

        assertEquals(1, instance.buffers.size());
        assertTrue(instance.buffers.containsKey(2L));
    }

     @Test
    public void testConsumereDelivery() throws Exception
    {
        final Channel channel       = mock(Channel.class);
        final FileSystem fs         = mock(FileSystem.class);
        final ShovelConfig cfg      = mock(ShovelConfig.class);
        final BufferedWriter b1     = mock(BufferedWriter.class);
        final BufferedWriter b2     = mock(BufferedWriter.class);
        final FSDataOutputStream os = mock(FSDataOutputStream.class);
        final Shovel instance       = new Shovel(channel, cfg){
            @Override
            protected FileSystem getFileSystem() throws IOException
            {
                return fs;
            }
        };

        final Consumer consumer                 = instance.createConsumer();
        final String consumerTag                = "fooTag";
        final Envelope envelope                 = mock(Envelope.class);
        final AMQP.BasicProperties properties   = mock(AMQP.BasicProperties.class);
        final byte[] body                       = "foo".getBytes();

        instance.buffers.put(1L, b1);
        instance.buffers.put(2L, b2);
        instance.tagReference.set(11L);

        when(cfg.getCurrentTime()).thenReturn(33L);
        when(envelope.getDeliveryTag()).thenReturn(333L);
        when(cfg.getTmpFileName(33L)).thenReturn("/tmp/33.tmp");
        when(fs.create(new Path("/tmp/33.tmp"), true)).thenReturn(os);

        consumer.handleDelivery(consumerTag, envelope, properties, body);

        verify(cfg).getCurrentTime();
        verify(cfg).getTmpFileName(33L);
        verify(fs).create(new Path("/tmp/33.tmp"), true);

        assertEquals(3, instance.buffers.size());
        assertTrue(instance.buffers.containsKey(33L));
        assertEquals(333L, (long)instance.tagReference.get());
    }

}
