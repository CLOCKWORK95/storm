package org.apache.storm.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class TestDummy {
    
    @Test
    public void testDummy() throws IOException{
        byte[] bytes = {1,2,3,4};
        ByteArrayInputStream bis= new ByteArrayInputStream(bytes);
        BufferInputStream buf = new BufferInputStream(bis, bis.available());
        byte[] readBytes = buf.read();
        assertEquals(bytes.length,readBytes.length);
        
    }
}
