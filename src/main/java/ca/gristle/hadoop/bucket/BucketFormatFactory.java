package ca.gristle.hadoop.bucket;

import ca.gristle.support.Utils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketFormatFactory {
    public static final String SEQUENCE_FILE = "SequenceFile";

    public static final String BUCKET_PATH_LISTER = "bucket.path.lister";

    public static void setBucketPathLister(JobConf conf, BucketPathLister lister) {
        Utils.setObject(conf, BUCKET_PATH_LISTER, lister);
    }

    public static List<Path> getBucketPaths(Bucket p, JobConf conf) throws IOException {
        BucketPathLister lister = (BucketPathLister) Utils.getObject(conf, BUCKET_PATH_LISTER);
        if(lister==null) lister = new AllBucketPathLister();
        return lister.getPaths(p);
    }

    public static BucketSpec getDefaultCopy() {
        return new BucketSpec(BucketFormatFactory.SEQUENCE_FILE);
    }

    public static BucketFormat create(BucketSpec spec) {
        if(spec==null || spec.getName()==null) spec = getDefaultCopy();
        String format = spec.getName();
        Map<String, Object> args = spec.getArgs();
        if(args==null) args = new HashMap<String, Object>();
        if(format.equals(SEQUENCE_FILE)) {
            return new SequenceFileFormat(args);
        } else {
            try {
                return (BucketFormat) Class.forName(format).newInstance();
            } catch(ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch(InstantiationException e) {
                throw new RuntimeException(e);
            } catch(IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
