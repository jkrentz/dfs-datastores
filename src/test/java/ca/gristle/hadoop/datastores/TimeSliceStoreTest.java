package ca.gristle.hadoop.datastores;

import ca.gristle.hadoop.bucket.Bucket;
import ca.gristle.support.FSTestCase;
import ca.gristle.support.TestUtils;
import ca.gristle.support.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class TimeSliceStoreTest extends FSTestCase {

    public static List readSlice(TimeSliceStore s, TimeSliceStore.Slice slice) throws IOException {
        List ret = new ArrayList();
        Iterator it = s.openRead(slice);
        Object o;
        while(it.hasNext()) {
            ret.add(it.next());
        }
        return ret;
    }

    public static void assertSliceContains(TimeSliceStore s, TimeSliceStore.Slice slice, Object... objs) throws IOException {
        Set objSet = new HashSet(readSlice(s, slice));
        Assertions.assertEquals(objSet.size(), objs.length);
        for(Object o: objs) {
            Assertions.assertTrue(objSet.contains(o));
        }
    }

    public static void writeSlice(TimeSliceStore s, TimeSliceStore.Slice slice, Object... objs) throws IOException {
        Bucket.TypedRecordOutputStream os = s.openWrite(slice);
        for(Object o: objs) {
            os.writeObject(o);
        }
        os.close();
        s.finishSlice(slice);
    }

    @Test
    public void testReadWrite() throws Exception {
        String tmp1 = TestUtils.getTmpPath(fs, "slices");
        TimeSliceStore<String> sliceStore = TimeSliceStore.create(fs, tmp1, new TimeSliceStringStructure());
        Assertions.assertNull(sliceStore.maxSliceStartSecs());
        Assertions.assertNull(sliceStore.minSliceStartSecs());

        try {
            sliceStore.openWrite(Utils.weekStartTime(100), Utils.weekStartTime(100)-1);
            Assertions.fail("should fail!");
        } catch(IllegalArgumentException e) {

        }

        try {
            sliceStore.openWrite(Utils.weekStartTime(100)-1, Utils.weekStartTime(100));
            Assertions.fail("should fail!");
        } catch(IllegalArgumentException e) {

        }
        TimeSliceStore.Slice slice = new TimeSliceStore.Slice(Utils.weekStartTime(100), Utils.weekStartTime(100));
        Bucket.TypedRecordOutputStream os = sliceStore.openWrite(slice);
        os.writeObject("a1");
        os.writeObject("a2");
        os.close();

        try {
            sliceStore.openRead(slice);
            Assertions.fail("should fail!");
        } catch(IllegalArgumentException e) {

        }
        Assertions.assertFalse(sliceStore.isSliceExists(slice));
        sliceStore.finishSlice(slice);
        Assertions.assertTrue(sliceStore.isSliceExists(slice));

        assertSliceContains(sliceStore, slice, "a1", "a2");

        try {
            sliceStore.openWrite(Utils.weekStartTime(99), Utils.weekStartTime(99)+1);
            Assertions.fail("should fail!");
        } catch(IllegalArgumentException e) {

        }

        TimeSliceStore.Slice slice2 = new TimeSliceStore.Slice(Utils.weekStartTime(100), Utils.weekStartTime(100)+10);
        os = sliceStore.openWrite(slice2);
        os.writeObject("b");
        os.close();
        sliceStore.finishSlice(slice2);

        assertSliceContains(sliceStore, slice, "a1", "a2");

        assertSliceContains(sliceStore, slice2, "b");

        try {
            sliceStore.openWrite(Utils.weekStartTime(100), Utils.weekStartTime(100)+1);
            Assertions.fail("should fail!");
        } catch(IllegalArgumentException e) {

        }

        Assertions.assertEquals(Utils.weekStartTime(100)+10, (int) sliceStore.maxSliceStartSecs());
        Assertions.assertEquals(Utils.weekStartTime(100), (int) sliceStore.minSliceStartSecs());

        try {
            sliceStore.finishSlice(Utils.weekStartTime(300), Utils.weekStartTime(299));
            Assertions.fail("should fail");
        } catch(IllegalArgumentException e) {

        }

        sliceStore.finishSlice(Utils.weekStartTime(200), Utils.weekStartTime(200) + 21);
        Assertions.assertEquals(Utils.weekStartTime(200) + 21, (int) sliceStore.maxSliceStartSecs());


    }

    protected static interface AppendOperation {
        public void append(TimeSliceStore dest, TimeSliceStore source) throws IOException;
    }

    public void appendTester(AppendOperation op) throws Exception {
        String path1 = TestUtils.getTmpPath(fs, "sliceStore1");
        String path2 = TestUtils.getTmpPath(fs, "sliceStoreNoAppend");
        String path3 = TestUtils.getTmpPath(fs, "sliceStoreAppend");

        TimeSliceStore base = TimeSliceStore.create(path1, new TimeSliceStringStructure());
        TimeSliceStore invalid = TimeSliceStore.create(path2, new TimeSliceStringStructure());
        TimeSliceStore valid = TimeSliceStore.create(path3, new TimeSliceStringStructure());

        writeSlice(base, new TimeSliceStore.Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa");
        writeSlice(invalid, new TimeSliceStore.Slice(Utils.weekStartTime(89), Utils.weekStartTime(89)+1), "bb");
        writeSlice(valid, new TimeSliceStore.Slice(Utils.weekStartTime(91), Utils.weekStartTime(91)+1), "cc");

        try {
            op.append(base, invalid);
            Assertions.fail("should fail!");
        } catch(IllegalArgumentException e) {

        }
        Assertions.assertEquals(1, base.getWeekStarts().size());
        Assertions.assertEquals(Utils.weekStartTime(90)+1, (int) base.minSliceStartSecs());
        Assertions.assertEquals(Utils.weekStartTime(90)+1, (int) base.maxSliceStartSecs());

        assertSliceContains(base, new TimeSliceStore.Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa");

        op.append(base, valid);

        Assertions.assertEquals(2, base.getWeekStarts().size());
        Assertions.assertEquals(Utils.weekStartTime(90)+1, (int) base.minSliceStartSecs());
        Assertions.assertEquals(Utils.weekStartTime(91)+1, (int) base.maxSliceStartSecs());
        assertSliceContains(base, new TimeSliceStore.Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa");
        assertSliceContains(base, new TimeSliceStore.Slice(Utils.weekStartTime(91), Utils.weekStartTime(91)+1), "cc");

    }

    @Test
    public void testCopyAppend() throws Exception {
        appendTester(new AppendOperation() {
            public void append(TimeSliceStore dest, TimeSliceStore source) throws IOException {
                dest.copyAppend(source);
            }
        });
    }

    @Test
    public void testMoveAppend() throws Exception {
        appendTester(new AppendOperation() {
            public void append(TimeSliceStore dest, TimeSliceStore source) throws IOException {
                dest.moveAppend(source);
            }
        });
    }

    @Test
    public void testAbsorb() throws Exception {
        appendTester(new AppendOperation() {
            public void append(TimeSliceStore dest, TimeSliceStore source) throws IOException {
                dest.absorb(source);
            }
        });
    }

    @Test
    public void testConsolidate() throws Exception {
        String tmp = TestUtils.getTmpPath(fs, "sliceStore");

        TimeSliceStore store = TimeSliceStore.create(tmp, new TimeSliceStringStructure());
        writeSlice(store, new TimeSliceStore.Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa", "bb");
        writeSlice(store, new TimeSliceStore.Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+2), "cc");

        TimeSliceStore.Slice slice = new TimeSliceStore.Slice(Utils.weekStartTime(100), Utils.weekStartTime(100));
        Bucket.TypedRecordOutputStream os = store.openWrite(slice);
        os.writeObject("dd");
        os.close();
        os = store.openWrite(slice);
        os.writeObject("ee");
        os.close();
        store.finishSlice(slice);

        store.consolidate();

        assertSliceContains(store, new TimeSliceStore.Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+1), "aa", "bb");
        assertSliceContains(store, new TimeSliceStore.Slice(Utils.weekStartTime(90), Utils.weekStartTime(90)+2), "cc");
        assertSliceContains(store, new TimeSliceStore.Slice(Utils.weekStartTime(100), Utils.weekStartTime(100)), "dd", "ee");
    }

    public static class TimeSliceStringStructure extends TimeSliceStructure<String> {
        public byte[] serialize(String str) {
            return str.getBytes();
        }

        public String deserialize(byte[] serialized) {
            return new String(serialized);
        }

        public Class getType() {
            return String.class;
        }
    }
}
