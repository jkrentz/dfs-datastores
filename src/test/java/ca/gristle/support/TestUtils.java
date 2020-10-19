package ca.gristle.support;

import ca.gristle.hadoop.formats.RecordInputStream;
import ca.gristle.hadoop.formats.RecordOutputStream;
import ca.gristle.hadoop.bucket.Bucket;
import com.google.common.collect.TreeMultiset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {

    private static final String TMP_ROOT = "/tmp/unittests";

    public static String getTmpPath(FileSystem fs, String name) throws IOException {
        fs.mkdirs(new Path(TMP_ROOT));
        String full = TMP_ROOT + "/" + name;
        if (fs.exists(new Path(full))) {
            fs.delete(new Path(full), true);
        }
        return full;
    }

    public static void deletePath(FileSystem fs, String path) throws IOException {
        fs.delete(new Path(path), true);
    }

    public static void emitToBucket(Bucket bucket, String file, Iterable<String> records) throws IOException {
        RecordOutputStream os = bucket.openWrite(file);
        for (String s: records) {
            os.writeRaw(s.getBytes());
        }
        os.close();
    }

    public static void emitToBucket(Bucket bucket, String file, String... records) throws IOException {
        RecordOutputStream os = bucket.openWrite(file);
        for (String s: records) {
            os.writeRaw(s.getBytes());
        }
        os.close();
    }

    public static void emitObjectsToBucket(Bucket bucket, Object... records) throws IOException {
        Bucket.TypedRecordOutputStream os = bucket.openWrite();
        for(Object r: records) {
            os.writeObject(r);
        }
        os.close();
    }

    public static void emitObjectsToBucket(Bucket bucket, List records) throws IOException {
        Bucket.TypedRecordOutputStream os = bucket.openWrite();
        for(Object r: records) {
            os.writeObject(r);
        }
        os.close();
    }


    public static List<String> getBucketRecords(Bucket bucket) throws IOException {
        List<String> ret = new ArrayList<String>();
        for(String s: bucket.getUserFileNames()) {
            RecordInputStream is = bucket.openRead(s);
            while(true) {
                byte[] r = is.readRawRecord();
                if(r==null) break;
                ret.add(new String(r));
            }
            is.close();
        }
        return ret;
    }

    public static <T> void assertBucketContents(Bucket<T> bucket, T... objects) {
        TreeMultiset contains = getBucketContents(bucket);
        TreeMultiset other = TreeMultiset.create();
        for(T obj: objects) {
            other.add(obj);
        }
        Assertions.assertEquals(other, contains, failureString(other, contains));
    }

    public static String failureString(Iterable expected, Iterable got) {
        String ret = "\n\nExpected:\n";
        for(Object o: expected) {
            ret = ret + o.toString() + "\n\n";
        }
        ret+="\nGot\n";
        for(Object o: got) {
            ret = ret + o.toString() + "\n\n";
        }
        ret+="\n\n";
        return ret;
    }

    public static void assertBucketContents(Bucket bucket, List objects) {
        TreeMultiset contains = getBucketContents(bucket);
        TreeMultiset other = TreeMultiset.create();
        for(Object obj: objects) {
            other.add(obj);
        }
        for(Object o: contains) {

        }
        Assertions.assertEquals(other, contains, failureString(other, contains));
    }


    public static <T> TreeMultiset<T> getBucketContents(Bucket<T> bucket) {
        TreeMultiset contains = TreeMultiset.create();
        for(T obj: bucket) {
            contains.add(obj);
        }
        return contains;
    }
}
