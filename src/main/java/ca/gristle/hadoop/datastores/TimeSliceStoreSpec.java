package ca.gristle.hadoop.datastores;

import ca.gristle.hadoop.bucket.BucketSpec;

import java.io.Serializable;
import java.util.Map;

public class TimeSliceStoreSpec implements Serializable {
    private BucketSpec _bucketSpec;

    public TimeSliceStoreSpec() {
        this(null, null);
    }

    public TimeSliceStoreSpec(TimeSliceStructure structure) {
        this(null, null, structure);
    }

    public TimeSliceStoreSpec(String format, Map<String, Object> args) {
        this(format, args, new DefaultTimeSliceStructure());
    }

    public TimeSliceStoreSpec(String format, Map<String, Object> args, TimeSliceStructure structure) {
        _bucketSpec = new BucketSpec(format, args, structure);
    }


    public BucketSpec toBucketSpec() {
        return _bucketSpec;
    }
}
