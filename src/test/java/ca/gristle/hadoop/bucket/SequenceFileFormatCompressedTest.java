package ca.gristle.hadoop.bucket;

public class SequenceFileFormatCompressedTest extends BucketFormatTester {

    public SequenceFileFormatCompressedTest() throws Exception {
        super();
    }

    @Override
    protected BucketSpec getSpec() {
        return new BucketSpec("SequenceFile").setArg("compressionType", "record").setArg("compressionCodec", "default");
    }

}
