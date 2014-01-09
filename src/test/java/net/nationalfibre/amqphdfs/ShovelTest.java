package net.nationalfibre.amqphdfs;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
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
        final Connection conn     = mock(Connection.class);
        final FileSystem fs       = mock(FileSystem.class);
        final ShovelConfig cfg    = mock(ShovelConfig.class);
        final BufferedWriter w1   = mock(BufferedWriter.class);
        final BufferedWriter w2   = mock(BufferedWriter.class);
        final Shovel.Buffer b1    = new Shovel.Buffer(1L, "/tmp/1.tmp", "/tmp/1", w1);
        final Shovel.Buffer b2    = new Shovel.Buffer(2L, "/tmp/2.tmp", "/tmp/2", w2);
        final Shovel instance     = new Shovel(channel, cfg) {
            @Override
            protected FileSystem getFileSystem() throws IOException
            {
                return fs;
            }
        };

        instance.buffers.put(b1.getWindow(), b1);
        instance.buffers.put(b2.getWindow(), b2);
        instance.tagReference.set(11L);

        when(cfg.getCurrentWindow()).thenReturn(2L);
        when(cfg.getGenerateUnique()).thenReturn("1111");
        when(cfg.getFileName("1111")).thenReturn(b1.getTarget());
        when(cfg.getTmpFileName("1111")).thenReturn(b1.getSource());

        when(conn.isOpen()).thenReturn(true);
        when(channel.getConnection()).thenReturn(conn);

        when(fs.exists(new Path(b1.getSource()))).thenReturn(true);
        when(fs.rename(new Path(b1.getSource()), new Path(b1.getTarget()))).thenReturn(true);

        instance.rotate();

        verify(w1).close();
        verify(w2, times(0)).close();
        verify(fs).rename(new Path(b1.getSource()), new Path(b1.getTarget()));
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
        final BufferedWriter w1     = mock(BufferedWriter.class);
        final BufferedWriter w2     = mock(BufferedWriter.class);
        final Shovel.Buffer b1      = new Shovel.Buffer(1L, "/tmp/1.tmp", "/tmp/1", w1);
        final Shovel.Buffer b2      = new Shovel.Buffer(2L, "/tmp/2.tmp", "/tmp/2", w2);
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

        when(cfg.getCurrentWindow()).thenReturn(33L);
        when(cfg.getGenerateUnique()).thenReturn("33");
        when(envelope.getDeliveryTag()).thenReturn(333L);
        when(cfg.getFileName("33")).thenReturn("/tmp/33");
        when(cfg.getTmpFileName("33")).thenReturn("/tmp/33.tmp");
        when(fs.create(new Path("/tmp/33.tmp"), true)).thenReturn(os);

        consumer.handleDelivery(consumerTag, envelope, properties, body);

        verify(cfg).getCurrentWindow();
        verify(cfg).getFileName("33");
        verify(cfg).getTmpFileName("33");
        verify(fs).create(new Path("/tmp/33.tmp"), true);

        assertEquals(3, instance.buffers.size());
        assertTrue(instance.buffers.containsKey(33L));
        assertEquals(333L, (long)instance.tagReference.get());
    }

    @Test
    public void testRotateBasedOnBufferPath() throws Exception
    {
        final Channel channel     = mock(Channel.class);
        final Connection conn     = mock(Connection.class);
        final FileSystem fs       = mock(FileSystem.class);
        final ShovelConfig cfg    = mock(ShovelConfig.class);
        final BufferedWriter w1   = mock(BufferedWriter.class);
        final Shovel.Buffer b1    = new Shovel.Buffer(1L, "/tmp/11111.tmp", "/tmp/11111", w1);
        final Shovel instance     = new Shovel(channel, cfg) {
            @Override
            protected FileSystem getFileSystem() throws IOException
            {
                return fs;
            }
        };

        instance.buffers.put(b1.getWindow(), b1);
        instance.tagReference.set(11L);

        when(cfg.getCurrentWindow()).thenReturn(2L);
        when(conn.isOpen()).thenReturn(true);
        when(channel.getConnection()).thenReturn(conn);

        when(fs.exists(new Path(b1.getSource()))).thenReturn(true);
        when(fs.rename(new Path(b1.getSource()), new Path(b1.getTarget()))).thenReturn(true);

        instance.rotate();

        verify(w1).close();
        verify(fs).rename(new Path(b1.getSource()), new Path(b1.getTarget()));
        verify(channel).basicAck(11L, true);

        assertTrue(instance.buffers.isEmpty());
    }

}
