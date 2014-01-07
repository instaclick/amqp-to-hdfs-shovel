package net.nationalfibre.amqphdfs;

import org.junit.Test;
import static org.junit.Assert.*;

public class ShovelConfigTest
{
    @Test
    public void testWindowsSizeAndCurrentTime()
    {
        ShovelConfig instance = ShovelConfig.create().withWindowsSize(10L);
        long expResult        = (System.currentTimeMillis() / 1000) / 10;

        assertEquals(10, (long)instance.getWindowsSize());
        assertEquals(expResult, instance.getCurrentTime());
    }

    @Test
    public void testFilePrefixAndCurrentTime()
    {
        String expResult      = "/logs/click-1000";
        ShovelConfig instance = ShovelConfig.create()
            .withFilePrefix("click-")
            .withRootPath("/logs/");

        assertEquals("click-", instance.getFilePrefix());
        assertEquals("/logs", instance.getRootPath());
        assertEquals(expResult, instance.getFileName("1000"));
        assertEquals(expResult+".tmp", instance.getTmpFileName("1000"));
    }
}
