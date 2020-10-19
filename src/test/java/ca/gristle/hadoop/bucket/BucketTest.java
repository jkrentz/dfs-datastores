package ca.gristle.hadoop.bucket;

import ca.gristle.hadoop.formats.RecordInputStream;
import ca.gristle.hadoop.formats.RecordOutputStream;
import ca.gristle.support.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class BucketTest {
    FileSystem local;

    public BucketTest() throws IOException {
        local = FileSystem.get(new Configuration());
    }

    @Test
    public void testCreation() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        Assertions.assertTrue(local.exists(new Path(path)));
        Assertions.assertTrue(local.exists(new Path(path, Bucket.META)));
        try {
            Bucket.create(local, path, true);
            Assertions.fail("should fail");
        } catch(Exception e) {}
        TestUtils.deletePath(local, path);
        BucketSpec spec = new BucketSpec(BucketFormatFactory.SEQUENCE_FILE)
                .setArg(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK)
                .setArg(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_GZIP);
        Bucket.create(local, path, spec);
        bucket = new Bucket(local, path);
        Assertions.assertEquals(spec, bucket.getSpec());

        Bucket.create(local, path + "/a/b", false);
        Assertions.assertTrue(local.exists(new Path(path, "a/b")));

        try {
            new Bucket(local, path + "/c");
            Assertions.fail("should fail");
        } catch(IllegalArgumentException e) {

        }
    }

    @Test
    public void testReadingWriting() throws Exception {
        readingWritingTestHelper(null);
        readingWritingTestHelper(new BucketSpec(BucketFormatFactory.SEQUENCE_FILE)
                .setArg(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK)
                .setArg(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_DEFAULT));
    }

    private void emitToBucket(Bucket bucket, String file, byte[]... records) throws Exception {
        RecordOutputStream os = bucket.openWrite(file);
        for(byte[] r: records) {
            os.writeRaw(r);
        }
        os.close();
    }

    private void checkContains(Bucket bucket, String file, byte[]... expected) throws Exception {
        RecordInputStream is = bucket.openRead(file);
        List<byte[]> records = new ArrayList<byte[]>();
        while(true) {
            byte[] arr = is.readRawRecord();
            if(arr==null) break;
            records.add(arr);
        }
        Assertions.assertEquals(expected.length, records.size());
        for(int i=0; i < expected.length; i++) {
            Assertions.assertArrayEquals(expected[i], records.get(i));
        }

    }

    private void readingWritingTestHelper(BucketSpec spec) throws Exception {
        byte[] r1 = new byte[] {1, 2, 3, 10};
        byte[] r2 = new byte[] {100};
        byte[] r3 = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
        byte[] r4 = new byte[] {};

        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path, spec);
        emitToBucket(bucket, "a", r1, r2, r3);
        emitToBucket(bucket, "a/b/c", r4);
        emitToBucket(bucket, "ddd", r3, r4, r1);

        checkContains(bucket, "a", r1, r2, r3);
        checkContains(bucket, "a/b/c", r4);
        checkContains(bucket, "ddd", r3, r4, r1);

        Set<String> userfiles = new HashSet<String>(bucket.getUserFileNames());
        Assertions.assertEquals(3, userfiles.size());
        Assertions.assertTrue(userfiles.contains("a"));
        Assertions.assertTrue(userfiles.contains("a/b/c"));
        Assertions.assertTrue(userfiles.contains("ddd"));
    }

    private void writeStrings(Bucket bucket, String userfile, String... strs) throws IOException {
        writeStrings(bucket, userfile, Arrays.asList(strs));
    }

    private void writeStrings(Bucket bucket, String userfile, Collection<String> strs) throws IOException {
        RecordOutputStream os = bucket.openWrite(userfile);
        for(String s: strs) {
            os.writeRaw(s.getBytes());
        }
        os.close();
    }

    public List<String> readWithIt(Bucket<byte[]> bucket) {
        List<String> returned = new ArrayList<String>();
        for(byte[] b: bucket) {
            returned.add(new String(b));
        }
        return returned;
    }

    @Test
    public void testIterator() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        Set<String> records = new HashSet<String>();
        records.add("aaaaa");
        records.add("bb");
        records.add("cccc");
        records.add("ddizizijjsjs");
        records.add("oqoqoqoq");
        writeStrings(bucket, "a", records);
        writeStrings(bucket, "a/b/c/d/eee", "nathan");
        records.add("nathan");
        Set<String> returned = new HashSet<String>(readWithIt(bucket));
        Assertions.assertEquals(records, returned);
    }

    @Test
    public void testAtomicity() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        RecordOutputStream os = bucket.openWrite("aaa");
        Assertions.assertEquals(0, bucket.getUserFileNames().size());
        os.writeRaw(new byte[] {1, 2, 3});
        os.close();
        Assertions.assertEquals(1, bucket.getUserFileNames().size());
    }

    public void checkStoredFiles(Bucket bucket, String... expected) throws Exception {
        Set<Path> paths = new HashSet<Path>(bucket.getStoredFiles());
        Assertions.assertEquals(expected.length, paths.size());
        for(String e: expected) {
            Assertions.assertTrue(paths.contains(new Path(e + Bucket.EXTENSION)), e);
        }
    }

    public void checkUserFiles(Bucket bucket, String... expected) throws Exception {
        Set<String> files = new HashSet<String>(bucket.getUserFileNames());
        Assertions.assertEquals(expected.length, files.size());
        for(String e: expected) {
            Assertions.assertTrue(files.contains(e), e);
        }
    }

    @Test
    public void testStoredFiles() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        emitToBucket(bucket, "a/b/c", new byte[] {1});
        checkStoredFiles(bucket, path + "/a/b/c");
        emitToBucket(bucket, "e", new byte[] {1});
        checkStoredFiles(bucket, path + "/a/b/c", path + "/e");
        emitToBucket(bucket, "100aaa", new byte[] {1});
        emitToBucket(bucket, "101/202/303", new byte[] {1});
        checkStoredFiles(bucket, path + "/a/b/c", path + "/e", path + "/100aaa", path + "/101/202/303");
    }

    @Test
    public void testSubBucket() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        emitToBucket(bucket, "a/b/c", new byte[] {1});
        emitToBucket(bucket, "e", new byte[] {1});
        checkUserFiles(bucket, "a/b/c", "e");
        Bucket s1 = bucket.getSubBucket("a");
        checkUserFiles(s1, "b/c");
        Bucket s2 = new Bucket(local, path + "/a/b");
        checkUserFiles(s2, "c");

        bucket.getSubBucket("asdasdasdasd");
        Assertions.assertTrue(local.exists(new Path(path, "asdasdasdasd")));
    }

    @Test
    public void testIsEmpty() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        Assertions.assertTrue(bucket.isEmpty());
        emitToBucket(bucket, "aaa");
        Assertions.assertTrue(bucket.isEmpty());
        emitToBucket(bucket, "bbb", new byte[] {1, 2, 3});
        Assertions.assertFalse(bucket.isEmpty());
        emitToBucket(bucket, "ccc", new byte[] {1, 2, 3});
        Assertions.assertFalse(bucket.isEmpty());
        bucket.delete("bbb");
        Assertions.assertFalse(bucket.isEmpty());
        bucket.delete("ccc");
        Assertions.assertTrue(bucket.isEmpty());
        bucket.delete("aaa");
        Assertions.assertTrue(bucket.isEmpty());
    }

    @Test
    public void testStructureConstructor() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket p = Bucket.create(local, path, new TestStructure());
        BucketSpec spec = p.getSpec();
        Assertions.assertNotNull(spec.getName());
        Assertions.assertEquals(TestStructure.class, spec.getStructure().getClass());
        //shouldn't throw exceptions...
        Bucket.create(local, path, new TestStructure(), false);
        Bucket.create(local, path, false);
        try {
            Bucket.create(local, path, new DefaultBucketStructure(), false);
            Assertions.fail("should throw exception");
        } catch(IllegalArgumentException e) {

        }
        path = TestUtils.getTmpPath(local, "bucket");
        Bucket.create(local, path);
        try {
            Bucket.create(local, path, new TestStructure(), false);
            Assertions.fail("should throw exception");
        } catch(IllegalArgumentException e) {

        }

    }

    protected List<byte[]> getRecords(Bucket p, String userfile) throws Exception {
        List<byte[]> ret = new ArrayList<byte[]>();
        RecordInputStream is = p.openRead(userfile);
        byte[] record;
        while((record = is.readRawRecord())!=null) {
            ret.add(record);
        }
        is.close();
        return ret;
    }

    @Test
    public void testStructured() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket<String> bucket = Bucket.create(local, path, BucketFormatFactory.getDefaultCopy().setStructure(new TestStructure()));
        Bucket<String>.TypedRecordOutputStream os = bucket.openWrite();
        os.writeObject("a1");
        os.writeObject("b1");
        os.writeObject("c1");
        os.writeObject("a2");
        os.writeObject("za1");
        os.writeObject("za2");
        os.writeObject("zb1");
        os.close();

        bucket = new Bucket(local, path);
        TestUtils.assertBucketContents(bucket, "a1", "b1", "c1", "a2", "za1", "za2", "zb1");
        TestUtils.assertBucketContents(bucket.getSubBucket("a"), "a1", "a2");
        TestUtils.assertBucketContents(bucket.getSubBucket("a/1"));
        TestUtils.assertBucketContents(bucket.getSubBucket("z"), "za1", "za2", "zb1");
        TestUtils.assertBucketContents(bucket.getSubBucket("z/a"), "za1", "za2");

        Bucket a = new Bucket(local, path + "/a");
        os = a.openWrite();
        os.writeObject("a2222");
        try {
            os.writeObject("zzz");
            Assertions.fail("should fail");
        } catch(IllegalStateException e) {

        }
        os.close();

        TestUtils.assertBucketContents(bucket, "a1", "b1", "c1", "a2", "za1", "za2", "zb1", "a2222");
        TestUtils.assertBucketContents(bucket.getSubBucket("a"), "a1", "a2", "a2222");
        TestUtils.assertBucketContents(bucket.getSubBucket("a/1"));
        TestUtils.assertBucketContents(bucket.getSubBucket("z"), "za1", "za2", "zb1");
        TestUtils.assertBucketContents(bucket.getSubBucket("z/a"), "za1", "za2");

    }

    @Test
    public void testMetadata() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        emitToBucket(bucket, "aaa", new byte[] {1});
        bucket.writeMetadata("aaa", "lalala");
        List<byte[]> recs = getRecords(bucket, "aaa");
        Assertions.assertEquals(1, recs.size());
        Assertions.assertTrue(Arrays.equals(recs.get(0), new byte[] {1}));
        Assertions.assertEquals("lalala", bucket.getMetadata("aaa"));

        bucket.writeMetadata("a/b", "whee");
        Assertions.assertEquals("whee", bucket.getMetadata("a/b"));
        Assertions.assertEquals("whee", new Bucket(local, path).getMetadata("a/b"));

        Assertions.assertNull(bucket.getMetadata("bbb"));

        List<String> md = bucket.getMetadataFileNames();
        Assertions.assertEquals(2, md.size());
        Assertions.assertTrue(md.contains("a/b"));
        Assertions.assertTrue(md.contains("aaa"));
    }

    @Test
    public void testAtRoot() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        Assertions.assertTrue(bucket.atRoot());
        Assertions.assertFalse(Bucket.create(local, path + "/a", false).atRoot());
    }

    @Test
    public void testMkAttr() throws Exception {
        String path = TestUtils.getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        bucket.mkAttr("a/b");
        List<String> attrs = bucket.getAttrsAtDir("a");
        Assertions.assertEquals(1, attrs.size());
        Assertions.assertEquals("b", attrs.get(0));
        Bucket bucket2 = new Bucket(local, path + "/a");
        bucket2.mkAttr("b/c");
        attrs = bucket.getAttrsAtDir("a/b");
        Assertions.assertEquals(1, attrs.size());
        Assertions.assertEquals("c", attrs.get(0));


    }
}
