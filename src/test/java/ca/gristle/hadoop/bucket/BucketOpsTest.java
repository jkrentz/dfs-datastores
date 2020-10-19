package ca.gristle.hadoop.bucket;

import ca.gristle.hadoop.formats.RecordInputStream;
import ca.gristle.hadoop.formats.RecordOutputStream;
import ca.gristle.hadoop.RenameMode;
import ca.gristle.support.FSTestCase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static ca.gristle.support.TestUtils.*;

public class BucketOpsTest extends FSTestCase {
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
    public void testDeleteSnapshot() throws IOException {
        String path = getTmpPath(local, "bucket");
        String snap = getTmpPath(fs, "snapshot");
        Bucket bucket = Bucket.create(local, path, new StringStructure());
        writeStrings(bucket, "aaa", "a", "b", "c", "d", "e");
        writeStrings(bucket, "bbb", "1", "2", "3");
        Bucket snapBucket = bucket.snapshot(snap);
        writeStrings(bucket, "aaa2", "a1");
        bucket.deleteSnapshot(snapBucket);
        assertBucketContents(bucket, "a1");
        List<String> names = bucket.getUserFileNames();
        Assertions.assertEquals("aaa2", names.get(0));
        Assertions.assertEquals(1, names.size());
    }

    @Test
    public void testClear() throws IOException {
        String path = getTmpPath(fs, "bucket");
        Bucket<String> bucket = Bucket.create(fs, path, BucketFormatFactory.getDefaultCopy().setStructure(new TestStructure()));
        Bucket<String>.TypedRecordOutputStream os = bucket.openWrite();
        os.writeObject("a1");
        os.writeObject("b1");
        os.writeObject("c1");
        os.writeObject("a2");
        os.writeObject("za1");
        os.writeObject("za2");
        os.writeObject("zb1");
        os.writeObject("a7");
        os.writeObject("a8");
        os.writeObject("za3");
        os.close();
        bucket.getSubBucket("a").clear();
        assertBucketContents(bucket, "b1", "c1", "za1", "za2", "zb1", "za3");
        bucket.clear();
        assertBucketContents(bucket);
    }

    @Test
    public void testConsolidationOne() throws Exception {
        String path = getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        writeStrings(bucket, "aaa", "a", "b", "c", "d", "e");
        writeStrings(bucket, "b/c/ddd", "1", "2", "3");
        writeStrings(bucket, "b/c/eee", "aaa", "bbb", "ccc", "ddd", "eee", "fff");
        writeStrings(bucket, "f", "z");
        writeStrings(bucket, "g", "zz");
        writeStrings(bucket, "h", "zzz");
        bucket.writeMetadata("a/b/qqq", "lalala");
        bucket.writeMetadata("f", "abc");
        bucket.consolidate();
        Assertions.assertEquals(1, bucket.getUserFileNames().size());
        Set<String> results = new HashSet<String>(readWithIt(bucket));
        Set<String> expected = new HashSet<String>(Arrays.asList("a", "b", "c", "d", "e",
                "1", "2", "3","aaa", "bbb", "ccc", "ddd", "eee", "fff","z", "zz", "zzz"));
        Assertions.assertEquals(expected, results);
        Assertions.assertEquals("abc", bucket.getMetadata("f"));
        Assertions.assertEquals("lalala", bucket.getMetadata("a/b/qqq"));
    }

    @Test
    public void testConsolidationMany() throws Exception {
        String path = getTmpPath(local, "bucket");
        Bucket bucket = Bucket.create(local, path);
        writeStrings(bucket, "aaa", "a", "b", "c", "d", "e");
        writeStrings(bucket, "b/c/ddd", "1", "2", "3");
        writeStrings(bucket, "b/c/eee", "aaa", "bbb", "ccc", "ddd", "eee", "fff");
        writeStrings(bucket, "f", "z");
        writeStrings(bucket, "g", "zz");
        writeStrings(bucket, "h", "zzz");
        long target = local.getContentSummary(bucket.toStoredPath("f")).getLength() +
                local.getContentSummary(bucket.toStoredPath("g")).getLength() + 1;
        bucket.consolidate(target);
        Assertions.assertTrue(bucket.getUserFileNames().size() < 6 && bucket.getUserFileNames().size() > 1);
        Set<String> results = new HashSet<String>(readWithIt(bucket));
        Set<String> expected = new HashSet<String>(Arrays.asList("a", "b", "c", "d", "e",
                "1", "2", "3","aaa", "bbb", "ccc", "ddd", "eee", "fff","z", "zz", "zzz"));
        Assertions.assertEquals(expected, results);
    }

    @Test
    public void testConsolidateStructured() throws Exception {
        String path = getTmpPath(fs, "bucket");
        Bucket<String> bucket = Bucket.create(fs, path, BucketFormatFactory.getDefaultCopy().setStructure(new TestStructure()));
        Bucket<String>.TypedRecordOutputStream os = bucket.openWrite();
        os.writeObject("a1");
        os.writeObject("b1");
        os.writeObject("c1");
        os.writeObject("a2");
        os.writeObject("za1");
        os.writeObject("za2");
        os.writeObject("zb1");
        os.close();
        os = bucket.openWrite();
        os.writeObject("a7");
        os.writeObject("a8");
        os.writeObject("za3");
        os.close();
        bucket.consolidate();
        assertBucketContents(bucket, "a1", "b1", "c1", "a2", "za1", "za2", "zb1", "a7", "a8", "za3");
        assertBucketContents(bucket.getSubBucket("a"), "a1", "a2", "a7", "a8");
        assertBucketContents(bucket.getSubBucket("z"), "za1", "za2", "zb1", "za3");
        assertBucketContents(bucket.getSubBucket("z/a"), "za1", "za2", "za3");
    }

    protected static interface AppendOperation {
        public void append(Bucket into, Bucket data, int renameMode) throws IOException;
        public void append(Bucket into, Bucket data, CopyArgs args) throws IOException;
        public boolean canAppendDifferentFormats();
    }

    private void basicAppendTest(AppendOperation op) throws Exception {
        String path1 = getTmpPath(fs, "bucket");
        String path2 = getTmpPath(fs, "bucket2");

        //test non structured append
        Bucket p1 = Bucket.create(fs, path1, new StringStructure());
        Bucket p2 = Bucket.create(fs, path2, new StringStructure());
        emitObjectsToBucket(p1, "aaa", "bbb", "ccc", "ddd");
        emitObjectsToBucket(p1, "eee", "fff", "ggg");
        emitObjectsToBucket(p2, "hhh");
        emitObjectsToBucket(p2, "iii");
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        assertBucketContents(p1, "aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg", "hhh", "iii");
    }

    private void renameAndStructureAppendTest(AppendOperation op, BucketSpec spec1, BucketSpec spec2) throws Exception {
        String path1 = getTmpPath(fs, "bucket");
        String path2 = getTmpPath(fs, "bucket2");

        spec1.setStructure(new StringStructure());
        spec2.setStructure(new StringStructure());
        //test non structured append
        Bucket p1 = Bucket.create(fs, path1, spec1);
        Bucket p2 = Bucket.create(fs, path2, spec2);
        emitToBucket(p1, "file1", "a");
        emitToBucket(p1, "1/file1", "b");
        emitToBucket(p1, "1/2/file1", "c");
        emitToBucket(p1, "2/file1", "d");
        emitToBucket(p2, "file1", "e");
        emitToBucket(p2, "1/file2", "f");
        emitToBucket(p2, "3/file1", "g");
        emitToBucket(p2, "1/file1", "h");

        try {
            op.append(p1, p2, RenameMode.NO_RENAME);
            Assertions.fail("should throw exception");
        } catch(Exception e) {

        }
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        assertBucketContents(p1, "a", "b", "c", "d", "e", "f", "g", "h");
        assertBucketContents(p1.getSubBucket("1"), "b", "c", "f", "h");
        Set<String> filenames = new HashSet<String>(p1.getUserFileNames());
        Assertions.assertEquals(8, filenames.size());
        Assertions.assertTrue(filenames.remove("file1"));
        Assertions.assertTrue(filenames.remove("1/file1"));
        Assertions.assertTrue(filenames.remove("1/2/file1"));
        Assertions.assertTrue(filenames.remove("1/file2"));
        Assertions.assertTrue(filenames.remove("2/file1"));
        Assertions.assertTrue(filenames.remove("3/file1"));

        Set<String> parents = new HashSet<String>();
        for(String f: filenames) {
            parents.add(new Path(f).getParent().toString());
        }
        Assertions.assertTrue(parents.contains(""));
        Assertions.assertTrue(parents.contains("1"));

        RecordInputStream in = p1.openRead("file1");
        String s = new String(in.readRawRecord());
        Assertions.assertTrue(in.readRawRecord()==null);
        in.close();
        Assertions.assertEquals("a", s);

    }

    public void appendTypeCompatibilityTest(AppendOperation op) throws Exception {
        String path1 = getTmpPath(fs, "bucket");
        String path2 = getTmpPath(fs, "bucket2");
        Bucket p1 = Bucket.create(fs, path1);
        Bucket p2 = Bucket.create(fs, path2, new StringStructure());
        emitToBucket(p1, "file1", "a", "b");
        emitObjectsToBucket(p2, "b");
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        try {
            op.append(p2, p1, RenameMode.RENAME_IF_NECESSARY);
            Assertions.fail("should throw exception");
        } catch(IllegalArgumentException e) {

        }
    }

    public void structureProtectionTest(AppendOperation op) throws Exception {
        String path1 = getTmpPath(fs, "bucket");
        String path2 = getTmpPath(fs, "bucket2");
        Bucket p1 = Bucket.create(fs, path1, new TestStructure());
        Bucket p2 = Bucket.create(fs, path2, new StringStructure());
        emitObjectsToBucket(p1, "a1", "a2", "b1", "c1", "z11", "z12", "z21");
        emitObjectsToBucket(p2, "c1", "d1", "e1");
        try {
            op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
            Assertions.fail("should throw exception");
        } catch(IllegalArgumentException e) {

        }

        try {
            op.append(p1.getSubBucket("z"), p2, RenameMode.RENAME_IF_NECESSARY);
            Assertions.fail("should throw exception");
        } catch(IllegalArgumentException e) {

        }
        assertBucketContents(p1, "a1", "a2", "b1", "c1", "z11", "z12", "z21");
        op.append(p1.getSubBucket("a"), p2, RenameMode.RENAME_IF_NECESSARY);
        assertBucketContents(p1, "a1", "a2", "b1", "c1", "z11", "z12", "z21", "c1", "d1", "e1");
        assertBucketContents(p1.getSubBucket("a"), "a1", "a2", "c1", "d1", "e1");



        path1 = getTmpPath(fs, "bucket");
        path2 = getTmpPath(fs, "bucket2");
        p1 = Bucket.create(fs, path1, new TestStructure());
        p2 = Bucket.create(fs, path2, new StringStructure());
        emitObjectsToBucket(p1, "b1", "za1");
        emitToBucket(p2, "a/file1", "a1", "a2");
        emitToBucket(p2, "b/file1", "b2");
        emitToBucket(p2, "z/a/file111111", "za2");
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        assertBucketContents(p1, "b1", "za1", "a1", "a2", "b2", "za2");
        assertBucketContents(p1.getSubBucket("b"), "b1", "b2");

        path2 = getTmpPath(fs, "bucket2");
        p2 = Bucket.create(fs, path2, new StringStructure());
        emitToBucket(p2, "z/b/file1", "zb1");
        op.append(p1, p2, RenameMode.RENAME_IF_NECESSARY);
        assertBucketContents(p1.getSubBucket("z/b"), "zb1");
    }

    public void metadataConflictTest(AppendOperation op) throws IOException {
        String path1 = getTmpPath(fs, "bucket");
        String path2 = getTmpPath(fs, "bucket2");

        //test non structured append
        Bucket p1 = Bucket.create(fs, path1, new StringStructure());
        Bucket p2 = Bucket.create(fs, path2, new StringStructure());

        emitToBucket(p1, "file1", "aaa");
        emitToBucket(p2, "file1", "bbb");
        emitToBucket(p2, "file2", "ccc");
        p1.writeMetadata("file1", "M1");
        p1.writeMetadata("a/b", "M2");
        p2.writeMetadata("file1", "M3");
        p2.writeMetadata("a/c", "M4");
        CopyArgs args = new CopyArgs();
        args.copyMetadata = false;
        op.append(p1, p2, args);

        assertBucketContents(p1, "aaa", "bbb", "ccc");
        Assertions.assertEquals("M1", p1.getMetadata("file1"));
        Assertions.assertEquals("M2", p1.getMetadata("a/b"));
        Assertions.assertNull(p1.getMetadata("a/c"));


        try {
            op.append(p1, p2, new CopyArgs());
            Assertions.fail("expected exception");
        } catch(IllegalArgumentException e) {

        }
    }

    public void metadataNonConflictTest(AppendOperation op) throws IOException {
        String path1 = getTmpPath(fs, "bucket");
        String path2 = getTmpPath(fs, "bucket2");
        String path3 = getTmpPath(fs, "bucket3");

        //test non structured append
        Bucket p1 = Bucket.create(fs, path1, new BucketSpec(BucketFormatFactory.SEQUENCE_FILE).setStructure(new StringStructure()));
        Bucket p2 = Bucket.create(fs, path2, new BucketSpec(BucketFormatFactory.SEQUENCE_FILE).setStructure(new StringStructure()));
        Bucket p3 = Bucket.create(fs, path3, new BucketSpec(BucketFormatFactory.SEQUENCE_FILE)
                    .setArg(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_DEFAULT)
                    .setArg(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK)
                    .setStructure(new StringStructure()));

        emitToBucket(p1, "meta3", "aaa");
        emitToBucket(p2, "file1", "bbb");
        emitToBucket(p3, "file2", "ccc");

        p1.writeMetadata("meta1", "M1");
        p2.writeMetadata("a/b/meta2", "M2");
        p3.writeMetadata("meta3", "M3");

        op.append(p1, p2, new CopyArgs());

        assertBucketContents(p1, "aaa", "bbb");
        Assertions.assertEquals("M1", p1.getMetadata("meta1"));
        Assertions.assertEquals("M2", p1.getMetadata("a/b/meta2"));


        if(op.canAppendDifferentFormats()) {
            System.out.println(p1.getStoredFilesAndMetadata());
            op.append(p1, p3, new CopyArgs());

            assertBucketContents(p1, "aaa", "bbb", "ccc");
            Assertions.assertEquals("M1", p1.getMetadata("meta1"));
            Assertions.assertEquals("M2", p1.getMetadata("a/b/meta2"));
            Assertions.assertEquals("M3", p1.getMetadata("meta3"));
        }
    }


    public void appendOperationTest(AppendOperation op) throws Exception {
        basicAppendTest(op);
        if(op.canAppendDifferentFormats())
            renameAndStructureAppendTest(op, new BucketSpec(BucketFormatFactory.SEQUENCE_FILE),
                    new BucketSpec(BucketFormatFactory.SEQUENCE_FILE)
                    .setArg(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_DEFAULT)
                    .setArg(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK));

        renameAndStructureAppendTest(op, new BucketSpec(BucketFormatFactory.SEQUENCE_FILE),
                new BucketSpec(BucketFormatFactory.SEQUENCE_FILE));
        appendTypeCompatibilityTest(op);
        structureProtectionTest(op);
        metadataConflictTest(op);
        metadataNonConflictTest(op);
    }

    @Test
    public void testCopyAppend() throws Exception {
        appendOperationTest(new AppendOperation() {
            public void append(Bucket into, Bucket data, int renameMode) throws IOException {
                into.copyAppend(data, renameMode);
            }

            public void append(Bucket into, Bucket data, CopyArgs args) throws IOException {
                into.copyAppend(data, args);
            }

            public boolean canAppendDifferentFormats() {
                return true;
            }
        });
    }

    @Test
    public void testMoveAppend() throws Exception {
        appendOperationTest(new AppendOperation() {
            public void append(Bucket into, Bucket data, int renameMode) throws IOException {
                into.moveAppend(data, renameMode);
            }

            public void append(Bucket into, Bucket data, CopyArgs args) throws IOException {
                into.moveAppend(data, args);
            }

            public boolean canAppendDifferentFormats() {
                return false;
            }
        });

        //TODO: test that original bucket is now empty
    }

    @Test
    public void testAbsorb() throws Exception {
        appendOperationTest(new AppendOperation() {
            public void append(Bucket into, Bucket data, int renameMode) throws IOException {
                into.absorb(data, renameMode);
            }

            public void append(Bucket into, Bucket data, CopyArgs args) throws IOException {
                into.absorb(data, args);
            }

            public boolean canAppendDifferentFormats() {
                return true;
            }
        });
    }


    public static class StringStructure implements BucketStructure<String> {

        public boolean isValidTarget(String... dirs) {
            return true;
        }

        public String deserialize(byte[] serialized) {
            return new String(serialized);
        }

        public byte[] serialize(String object) {
            return object.getBytes();
        }

        public List<String> getTarget(String object) {
            return Collections.EMPTY_LIST;
        }

        public Class getType() {
            return String.class;
        }

    }
}
