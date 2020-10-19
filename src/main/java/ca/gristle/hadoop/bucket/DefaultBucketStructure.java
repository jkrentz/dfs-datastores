package ca.gristle.hadoop.bucket;

import java.util.Collections;
import java.util.List;

public class DefaultBucketStructure extends BinaryBucketStructure {

    public boolean isValidTarget(String... dirs) {
        return true;
    }

    public List<String> getTarget(byte[] object) {
        return Collections.EMPTY_LIST;
    }
}
