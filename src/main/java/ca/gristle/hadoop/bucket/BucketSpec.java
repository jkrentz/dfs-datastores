package ca.gristle.hadoop.bucket;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class BucketSpec implements Writable, Serializable {
    private String name;
    private Map<String, Object> args;
    private BucketStructure structure;

    private static final BucketStructure DEFAULT_STRUCTURE = new DefaultBucketStructure();

    public BucketSpec() {

    }

    public BucketSpec(String name) {
        this(name, (BucketStructure)null);
    }

    public BucketSpec(String name, BucketStructure structure) {
        this(name, new HashMap<String, Object>(), structure);
    }

    public BucketSpec(String name, Map<String, Object> args) {
        this(name, args, null);
    }

    public BucketSpec(String name, Map<String, Object> args, BucketStructure structure) {
        this.name = name;
        this.args = args == null ? null : new HashMap(args);
        this.structure = structure;
    }

    public BucketSpec(BucketStructure structure) {
        this(null, null, structure);
    }

    public BucketSpec setStructure(BucketStructure structure) {
        this.structure = structure;
        return this;
    }

    public BucketSpec setArg(String arg, Object val) {
        this.args.put(arg, val);
        return this;
    }

    @Override
    public String toString() {
        return mapify().toString();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof BucketSpec)) return false;
        BucketSpec ps = (BucketSpec) obj;
        return name.equals(ps.name) &&
               args.equals(ps.args) &&
               getStructure().getClass().equals(ps.getStructure().getClass());
    }

    @Override
    public int hashCode() {
        return name.hashCode() + args.hashCode();
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getArgs() {
        return args;
    }

    public BucketStructure getStructure() {
        if(structure == null) return DEFAULT_STRUCTURE;
        else return structure;
    }


    public static BucketSpec readFromFileSystem(FileSystem fs, Path path) throws IOException {
        FSDataInputStream is = fs.open(path);
        BucketSpec ret = parseFromStream(is);
        is.close();
        return ret;
    }

    public static BucketSpec parseFromStream(InputStream is) {
        Map format = new Yaml().load(new InputStreamReader(is));
        return parseFromMap(format);
    }

    protected static BucketStructure getStructureFromClass(String klass) {
        if(klass==null) return null;
        Class c;
        try {
            c = Class.forName(klass);
            return (BucketStructure) c.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate BucketStructure class " + klass, e);
        }
    }

    protected static BucketSpec parseFromMap(Map<String, Object> format) {
        String name = (String) format.get("format");
        Map<String, Object> args = (Map<String, Object>) format.get("args");
        String structClass = (String) format.get("structure");
        return new BucketSpec(name, args, getStructureFromClass(structClass));
    }

    public void writeToStream(OutputStream os) {
        new Yaml().dump(mapify(), new OutputStreamWriter(os));
    }

    private Map<String, Object> mapify() {
        Map<String, Object> format = new HashMap<String, Object>();
        format.put("format", name);
        format.put("args", args);
        if(structure!=null) {
            format.put("structure", structure.getClass().getName());
        }
        return format;
    }

    public void writeToFileSystem(FileSystem fs, Path path) throws IOException {
        FSDataOutputStream os = fs.create(path);
        writeToStream(os);
        os.close();
    }

    public void write(DataOutput d) throws IOException {
        String ser = new Yaml().dump(mapify());
        WritableUtils.writeString(d, ser);
    }

    public void readFields(DataInput di) throws IOException {
        BucketSpec spec = parseFromMap(new Yaml().load(WritableUtils.readString(di)));
        this.name = spec.name;
        this.args = spec.args;
    }
}
