package ca.gristle.hadoop.bucket;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;


public class AllBucketPathLister implements BucketPathLister {
    public List<Path> getPaths(Bucket p) throws IOException {
        return p.getStoredFiles();
    }

}
