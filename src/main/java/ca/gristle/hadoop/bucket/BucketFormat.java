package ca.gristle.hadoop.bucket;

import ca.gristle.hadoop.formats.RecordStreamFactory;
import org.apache.hadoop.mapred.InputFormat;

public interface BucketFormat extends RecordStreamFactory {
    public Class<? extends InputFormat> getInputFormatClass();
}
