package ca.gristle.hadoop.bucket;

public class SequenceFileFormatTest extends BucketFormatTester {

    public SequenceFileFormatTest() throws Exception {
        super();
    }

    @Override
    protected BucketSpec getSpec() {
        return new BucketSpec("SequenceFile");
    }

}
