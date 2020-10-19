package ca.gristle.hadoop.bucket;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface BucketPathLister extends Serializable {
    public List<Path> getPaths(Bucket p) throws IOException;
}
