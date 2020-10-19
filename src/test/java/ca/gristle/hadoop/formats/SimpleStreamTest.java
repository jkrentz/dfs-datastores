package ca.gristle.hadoop.formats;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import ca.gristle.support.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class SimpleStreamTest {

    @Test
    public void testSimpleStreams() throws IOException {
        FileSystem fs = FileSystem.getLocal(new Configuration());
        String path = TestUtils.getTmpPath(fs, "simplestreams");
        SimpleOutputStream sos = new SimpleOutputStream(new FileOutputStream(path));
        for(int i=0; i<=10000; i++) {
            sos.writeRaw(("prefix" + i + "suffix").getBytes());
        }
        sos.close();

        SimpleInputStream is = new SimpleInputStream(new FileInputStream(path));
        for(int i=0; i<=10000; i++) {
            Assertions.assertArrayEquals(("prefix" + i + "suffix").getBytes(), is.readRawRecord());
        }
        Assertions.assertNull(is.readRawRecord());
        Assertions.assertNull(is.readRawRecord());
        Assertions.assertNull(is.readRawRecord());
        Assertions.assertNull(is.readRawRecord());
        Assertions.assertNull(is.readRawRecord());
        is.close();
    }
}
