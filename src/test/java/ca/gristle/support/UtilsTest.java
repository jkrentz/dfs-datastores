package ca.gristle.support;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UtilsTest {

    @Test
    public void testGetBytes() {
        BytesWritable b = new BytesWritable(new byte[] {1, 2, 3});
        Assertions.assertArrayEquals(new byte[] {1, 2, 3}, Utils.getBytes(b));
        b.set(new byte[] {1}, 0, 1);
        Assertions.assertArrayEquals(new byte[] {1}, Utils.getBytes(b));
    }

    @Test
    public void testJoin() {
        Assertions.assertEquals("a/b/ccc/d", Utils.join(new String[] { "a", "b", "ccc", "d"}, "/"));
        Assertions.assertEquals("1", Utils.join(new String[] {"1"}, "/asdads"));

        List<Integer> args = new ArrayList<Integer>() {{
            add(1);
            add(2);
            add(10);
        }};
        Assertions.assertEquals("1, 2, 10", Utils.join(args, ", "));
    }

    @Test
    public void testFill() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        byte[] buf = new byte[10];
        Assertions.assertEquals(8, Utils.fill(is, buf));
        for(int i=1; i<=8; i++) {
            Assertions.assertEquals(i, buf[i-1]);
        }
        is = new ByteArrayInputStream(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        buf = new byte[4];
        Assertions.assertEquals(4, Utils.fill(is, buf));
        for(int i=1; i<=4; i++) {
            Assertions.assertEquals(i, buf[i-1]);
        }
    }

    @Test
    public void testFirstNBytesSame() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        String path1 = TestUtils.getTmpPath(fs, "file1");
        String path2 = TestUtils.getTmpPath(fs, "file2");

        FSDataOutputStream os = fs.create(new Path(path1));
        os.write(new byte[] {1, 2, 3, 4, 10, 11, 12});
        os.close();

        os = fs.create(new Path(path2));
        os.write(new byte[] {1, 2, 3, 4, 5, 6});
        os.close();

        Assertions.assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 3));
        Assertions.assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 4));
        Assertions.assertFalse(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 5));
        Assertions.assertFalse(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 1000));
    }

    @Test
    public void testFirstNBytesFileLength() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        String path1 = TestUtils.getTmpPath(fs, "file1");
        String path2 = TestUtils.getTmpPath(fs, "file2");

        FSDataOutputStream os = fs.create(new Path(path1));
        os.write(new byte[] {1, 2, 2, 3, 3});
        os.close();

        os = fs.create(new Path(path2));
        os.write(new byte[] {1, 2, 2, 3, 3, 4});
        os.close();
        Assertions.assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 3));
        Assertions.assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 4));
        Assertions.assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 5));
        Assertions.assertFalse(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 1000));

        path2 = TestUtils.getTmpPath(fs, "file2");
        os = fs.create(new Path(path2));
        os.write(new byte[] {1, 2, 2, 3, 3});
        os.close();
        Assertions.assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 3));
        Assertions.assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 4));
        Assertions.assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 5));
        Assertions.assertTrue(Utils.firstNBytesSame(fs, new Path(path1), fs, new Path(path2), 1000));


    }
}
