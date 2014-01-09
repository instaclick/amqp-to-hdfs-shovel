package net.nationalfibre.amqphdfs;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ShovelManagerTest
{
    @Test
    public void testStart() throws IOException
    {
        final Channel c1       = mock(Channel.class);
        final Connection conn  = mock(Connection.class);
        final Runnable rotator = mock(Runnable.class);
        final ShovelConfig cfg = mock(ShovelConfig.class);

        final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        final ShovelManager instance             = new ShovelManager(conn, scheduler) {
            @Override
            protected Runnable createRotator(Shovel shovel)
            {
                return rotator;
            }
        };

        when(conn.createChannel()).thenReturn(c1);
        when(cfg.getWindowsSize()).thenReturn(11L);
        when(cfg.getQueueName()).thenReturn("a1");

        instance.start(cfg);

        verify(conn).createChannel();
        verify(scheduler).scheduleAtFixedRate(rotator, 11L, 11L, TimeUnit.SECONDS);

        assertEquals(1, instance.map.size());
        assertTrue(instance.map.containsKey("a1"));
    }

    @Test
    public void testStop() throws IOException
    {
        final Shovel s1       = mock(Shovel.class);
        final Shovel s2       = mock(Shovel.class);
        final Channel c1      = mock(Channel.class);
        final Channel c2      = mock(Channel.class);
        final Connection conn = mock(Connection.class);

        final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        final ShovelManager instance             = new ShovelManager(conn, scheduler);

        when(s1.getChannel()).thenReturn(c1);
        when(s2.getChannel()).thenReturn(c2);

        instance.map.put("s1", s1);
        instance.map.put("s2", s2);

        instance.stop();

        verify(s1).flush(-1L);
        verify(s2).flush(-1L);

        verify(s1).getChannel();
        verify(s2).getChannel();

        verify(c1).close();
        verify(c2).close();
        verify(conn).close();
    }

}
