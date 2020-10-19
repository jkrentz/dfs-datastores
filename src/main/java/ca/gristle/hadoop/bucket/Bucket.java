package ca.gristle.hadoop.bucket;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import ca.gristle.hadoop.Coercer;
import ca.gristle.hadoop.PathLister;
import ca.gristle.hadoop.RenameMode;
import ca.gristle.hadoop.formats.RecordInputStream;
import ca.gristle.hadoop.formats.RecordOutputStream;
import ca.gristle.support.Utils;
import ca.gristle.hadoop.BalancedDistcp;
import ca.gristle.hadoop.Consolidator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bucket<T> extends AbstractBucket implements Iterable<T>{
    public static Logger LOG = LoggerFactory.getLogger(Bucket.class);

    public static final String META = "bucket.meta";

    public class TypedRecordOutputStream implements RecordOutputStream {
        private HashMap<String, RecordOutputStream> _workers = new HashMap<String, RecordOutputStream>();
        private String _userfilename;
        private boolean _overwrite;

        public TypedRecordOutputStream(String userfilename, boolean overwrite) {
            _userfilename = userfilename;
            _overwrite = overwrite;
        }

        public <T> void writeObject(T obj) throws IOException {
            BucketStructure<T> structure = ((BucketStructure<T>) _structure);
            List<String> rootAttrs = structure.getTarget(obj);
            List<String> attrs = makeRelative(rootAttrs);
            String targetDir = Utils.join(attrs, "/");
            if(!_workers.containsKey(targetDir)) {
                Path p;
                if(targetDir.length()==0) p = new Path(_userfilename);
                else p = new Path(targetDir, _userfilename);
                List<String> totalAttrs = componentsFromRoot(p.toString());
                if(!_structure.isValidTarget(totalAttrs.toArray(new String[totalAttrs.size()]))) {
                    throw new IllegalArgumentException("Cannot write object " + obj.toString() + " to " + p.toString() +
                            ". Conflicts with the structure of the datastore.");
                }
                _workers.put(targetDir, Bucket.super.openWrite(p.toString(), _overwrite));
            }
            RecordOutputStream os = _workers.get(targetDir);
            os.writeRaw(structure.serialize(obj));
        }

        public void writeObjects(T... objs) throws IOException {
            for(T obj: objs) {
                writeObject(obj);
            }
        }

        public void close() throws IOException {
            for(RecordOutputStream os: _workers.values()) {
                os.close();
            }
        }

        protected List<String> makeRelative(List<String> attrs) {
            return Utils.stripRoot(getAttrs(), attrs);
        }

        public void writeRaw(byte[] record) throws IOException {
            writeRaw(record, 0, record.length);
        }

        public void writeRaw(byte[] record, int start, int length) throws IOException {
            if(!_workers.containsKey(_userfilename)) {
                checkValidStructure(_userfilename);
                _workers.put(_userfilename, Bucket.super.openWrite(_userfilename, _overwrite));
            }
            _workers.get(_userfilename).writeRaw(record, start, length);
        }
    }


    public class TypedRecordInputStream implements RecordInputStream {
        private RecordInputStream is;

        public TypedRecordInputStream(String userFileName) throws IOException {
            is = Bucket.super.openRead(userFileName);
        }

        public T readObject() throws IOException {
            byte[] record = readRawRecord();
            if(record==null) return null;
            else return _structure.deserialize(record);
        }

        public void close() throws IOException {
            is.close();
        }

        public byte[] readRawRecord() throws IOException {
            return is.readRawRecord();
        }
    }

    public static Bucket create(String path, BucketSpec spec) throws IOException {
        return create(Utils.getFS(path), path, spec);
    }

    public static Bucket create(FileSystem fs, String path, BucketSpec spec) throws IOException {
        return create(fs, path, spec, true);
    }

    public static Bucket create(String path) throws IOException {
        return create(Utils.getFS(path), path);
    }

    public static Bucket create(FileSystem fs, String path) throws IOException {
        return create(fs, path, (BucketSpec) null);
    }

    public static Bucket create(String path, BucketStructure structure) throws IOException {
        return create(Utils.getFS(path), path, structure);
    }

    public static Bucket create(FileSystem fs, String path, BucketStructure structure) throws IOException {
        return create(fs, path, new BucketSpec(structure));
    }

    public static Bucket create(String path, BucketStructure structure, boolean failOnExists) throws IOException {
        return create(Utils.getFS(path), path, structure, failOnExists);
    }

    public static Bucket create(FileSystem fs, String path, BucketStructure structure, boolean failOnExists) throws IOException {
        return create(fs, path, new BucketSpec(structure), failOnExists);
    }

    public static Bucket create(String path, boolean failOnExists) throws IOException {
        return create(Utils.getFS(path), path, failOnExists);
    }

    public static Bucket create(FileSystem fs, String path, boolean failOnExists) throws IOException {
        return create(fs, path, (BucketSpec) null, failOnExists);
    }

    public static Bucket create(String path, BucketSpec spec, boolean failOnExists) throws IOException {
        return create(Utils.getFS(path), path, spec, failOnExists);
    }

    public static Bucket create(FileSystem fs, String path, BucketSpec spec, boolean failOnExists) throws IOException {
        Path pathp = new Path(path);
        BucketFormatFactory.create(spec);
        BucketSpec existing = getSpec(fs, pathp);
        if(failOnExists) {
            if(existing!=null) {
                throw new IllegalArgumentException("Bucket already exists at path " + path + " with spec " + existing.toString());
            }
            if(fs.exists(pathp))
                throw new IllegalArgumentException("Path " + path + " already exists");
        }
        if(spec!=null && existing!=null) {
            if(spec.getName()!=null) {
                if(!spec.equals(existing))
                    throw new IllegalArgumentException("Specs do not match " + spec.toString() + ", " + existing.toString());
            } else if(spec.getStructure()!=null) {
                if(existing.getStructure()==null || !spec.getStructure().getClass().equals(existing.getStructure().getClass())) {
                    throw new IllegalArgumentException("Specs do not match " + spec.toString() + ", " + existing.toString());
                }
            }
        }
        fs.mkdirs(pathp);
        if(existing==null) {
            if(spec==null) spec = BucketFormatFactory.getDefaultCopy();
            if(spec.getName()==null) spec = BucketFormatFactory.getDefaultCopy().setStructure(spec.getStructure());
            spec.writeToFileSystem(fs, new Path(pathp, META));
        }


        return new Bucket(fs, path);
    }

    private static BucketSpec getSpec(FileSystem fs, Path path) throws IOException {
        return (BucketSpec) getSpecAndRoot(fs, path)[1];
    }

    private static String getRoot(FileSystem fs, Path path) throws IOException {
        return (String) getSpecAndRoot(fs, path)[0];
    }

    private static Object[] getSpecAndRoot(FileSystem fs, Path path) throws IOException {
        Path curr = path;
        Object[] ret = null;
        while( curr != null ) { //  changed as per oscar
        //while(true) {
            Path meta = new Path(curr, META);
            if(fs.exists(meta)) {
                if(ret!=null) throw new RuntimeException("At least two meta files up directory tree");
                BucketSpec spec = BucketSpec.readFromFileSystem(fs, meta);
                ret = new Object[] {curr.toString(), spec};
            }
            if(curr.depth()==0) break;
            curr = curr.getParent();
        }
        if(ret==null) ret = new Object[] {null, null};
        return ret;
    }


    private BucketFormat _format;
    private BucketSpec _spec;
    private BucketStructure<T> _structure;
    private String _root;
    private FileSystem _fs;

    public Bucket(String path) throws IOException {
        this(Utils.getFS(path), path);
    }

    public Bucket(String path, Configuration conf) throws IOException {
        this(Utils.getFS(path, conf), path);
    }

    public Bucket(FileSystem fs, String path) throws IOException {
        super(path);
        _fs = fs;
        _root = getRoot(fs, new Path(path));
        if(_root==null || !fs.exists(new Path(path)))
            throw new IllegalArgumentException("Bucket does not exist at path " + path);
        _spec = getSpec(fs, new Path(path));
        _structure = _spec.getStructure();
        _format = BucketFormatFactory.create(_spec);
    }

    public FileSystem getFileSystem() {
        return _fs;
    }


    public TypedRecordOutputStream openWrite() throws IOException {
        return openWrite(UUID.randomUUID().toString(), false);
    }

    @Override
    public TypedRecordOutputStream openWrite(String subFileName, boolean overwrite) throws IOException {
        if(subFileName.contains(META)) throw new IllegalArgumentException("Illegal user file name " + subFileName);
        checkPathValidity(subFileName);
        return new TypedRecordOutputStream(subFileName, overwrite);
    }

    @Override
    public TypedRecordInputStream openRead(String userfilename) throws IOException {
        checkPathValidity(userfilename);
        checkValidStructure(userfilename);
        return new TypedRecordInputStream(userfilename);
    }

    protected void checkPathValidity(String subFileName) {
        List<String> components = Utils.componentize(subFileName);
        for(String s: components) {
            if(s.startsWith("_")) {
                throw new IllegalArgumentException("Cannot have underscores in path names " + subFileName);
            }
        }
    }

    public Bucket<T> getSubBucket(int... attrs) throws IOException {
        List<String> elems = new ArrayList<String>();
        for(int i: attrs) {
            elems.add("" + i);
        }
        String relPath = Utils.join(elems, "/");
        return getSubBucket(relPath);
    }

    public Bucket<T> getSubBucket(String relpath) throws IOException {
        mkdirs(new Path(getInstanceRoot(), relpath));
        return new Bucket(_fs, new Path(getInstanceRoot(), relpath).toString());
    }

    public BucketSpec getSpec() {
        return _spec;
    }

    public BucketFormat getFormat() {
        return _format;
    }

    public String getRoot() {
        return _root;
    }

    public boolean atRoot() {
        Path instanceRoot = new Path(getInstanceRoot()).makeQualified(_fs);
        Path root = new Path(getRoot()).makeQualified(_fs);
        return root.equals(instanceRoot);
    }

    public List<String> getAttrs() {
        return Utils.stripRoot(Utils.componentize(getRoot()), Utils.componentize(getInstanceRoot()));
    }

    //returns if formats are same
    private boolean checkCombineValidity(Bucket p, CopyArgs args) throws IOException {
        if(args.force) return true;
        BucketSpec mine = getSpec();
        BucketSpec other = p.getSpec();
        BucketStructure structure = mine.getStructure();

        boolean typesSame = structure.getType().equals(other.getStructure().getType());
        //can always append into a "raw" bucket
        if(!structure.getType().equals(new byte[0].getClass()) && !typesSame)
            throw new IllegalArgumentException("Cannot combine two buckets of different types unless target bucket is raw");

        //check that structure will be maintained
        for(String name: p.getUserFileNames()) {
            checkValidStructure(name);
        }


        return mine.getName().equals(other.getName()) && mine.getArgs().equals(other.getArgs());
    }

    public Bucket snapshot(Configuration configuration, FileSystem fileSystem, String path) throws IOException {
        Bucket ret = createEmptyMimic(fileSystem, path);
        ret.copyAppend(this, RenameMode.NO_RENAME, configuration);
        return ret;
    }

    public Bucket snapshot(FileSystem fileSystem, String path) throws IOException {
        Bucket ret = createEmptyMimic(fileSystem, path);
        ret.copyAppend(this, RenameMode.NO_RENAME);
        return ret;
    }

    public Bucket snapshot(String path) throws IOException {
        Bucket ret = createEmptyMimic(path);
        ret.copyAppend(this, RenameMode.NO_RENAME);
        return ret;
    }

    public void clear() throws IOException {
        for(Path p: getStoredFiles()) {
            delete(p, false);
        }
    }

    public void deleteSnapshot(Bucket snapshot) throws IOException {
        for(String username: snapshot.getUserFileNames()) {
            delete(username);
        }
    }

    public Bucket createEmptyMimic(FileSystem fileSystem, String path) throws IOException {
        if(getSpec(fileSystem, new Path(path))!=null) {
            throw new IllegalArgumentException("Cannot make empty mimic at " + path + " because it is a subdir of a bucket");
        }
        if(fileSystem.exists(new Path(path))) {
            throw new IllegalArgumentException(path + " already exists");
        }
        return Bucket.create(fileSystem, path, getSpec(), true);
    }

    public Bucket createEmptyMimic(String path) throws IOException {
        return createEmptyMimic(Utils.getFS(path), path);
    }

    public void coerce(String path, String name, Map<String, Object> args) throws IOException {
        create(path, new BucketSpec(name, args).setStructure(getSpec().getStructure())).copyAppend(this);
    }

    public void coerce(FileSystem fs, String path, String name, Map<String, Object> args) throws IOException {
        create(fs, path, new BucketSpec(name, args).setStructure(getSpec().getStructure())).copyAppend(this);
    }


    public void copyAppend(Bucket p) throws IOException {
        copyAppend(p, new CopyArgs());
    }

    public void copyAppend(Bucket p, int renameMode) throws IOException {
        CopyArgs args = new CopyArgs();
        args.renameMode = renameMode;
        copyAppend(p, args);
    }

    public void copyAppend(Bucket p, int renameMode, Configuration configuration) throws IOException {
        CopyArgs args = new CopyArgs();
        args.renameMode = renameMode;
        args.configuration = configuration;
        copyAppend(p, args);
    }

    protected String getQualifiedRoot(Bucket p) {
        Path path = new Path(p.getInstanceRoot());
        return path.makeQualified(p._fs).toString();
    }
    /**
     * Copy append will copy all the files from p into this bucket. Appending maintains the
     * structure that was present in p.
     *
     */
    public void copyAppend(Bucket p, CopyArgs args) throws IOException {
        args = new CopyArgs(args);
        if(args.renameMode==null) args.renameMode = RenameMode.ALWAYS_RENAME;

        boolean formatsSame = checkCombineValidity(p, args);
        String sourceQual = getQualifiedRoot(p);
        String destQual = getQualifiedRoot(this);
        if(formatsSame) {
            BalancedDistcp.distcp(sourceQual, destQual, args.renameMode, new BucketPathLister(args.copyMetadata), EXTENSION, args.configuration);
        } else {
            Coercer.coerce(sourceQual, destQual, args.renameMode, new BucketPathLister(args.copyMetadata), p.getFormat(), getFormat(), EXTENSION, args.configuration);
        }
    }

    public void moveAppend(Bucket p) throws IOException {
        moveAppend(p, new CopyArgs());
    }

    public void moveAppend(Bucket p, int renameMode) throws IOException {
        CopyArgs args = new CopyArgs();
        args.renameMode = renameMode;
        moveAppend(p, args);
    }

    public void moveAppend(Bucket p, CopyArgs args) throws IOException {
        args = new CopyArgs(args);
        if(args.renameMode==null) args.renameMode = RenameMode.ALWAYS_RENAME;
        boolean formatsSame = checkCombineValidity(p, args);
        if(!p._fs.getUri().equals(_fs.getUri())) throw new IllegalArgumentException("Cannot move append between different filesystems");
        if(!formatsSame) throw new IllegalArgumentException("Cannot move append different format buckets together");

        for(String name: p.getUserFileNames()) {
            String parent = new Path(name).getParent().toString();
            _fs.mkdirs(new Path(getInstanceRoot() + "/" + parent));
            Path storedPath = p.toStoredPath(name);
            Path targetPath = toStoredPath(name);
            if(_fs.exists(targetPath) || args.renameMode == RenameMode.ALWAYS_RENAME) {
                if(args.renameMode == RenameMode.NO_RENAME)
                    throw new IllegalArgumentException("Collision of filenames " + targetPath.toString());
                if(parent.equals("")) targetPath = toStoredPath("ma_" + UUID.randomUUID().toString());
                else targetPath = toStoredPath(parent + "/ma_" + UUID.randomUUID().toString());
            }
            _fs.rename(storedPath, targetPath);
        }

        if(args.copyMetadata) {
            for(String metaName: p.getMetadataFileNames()) {
                Path source = p.toStoredMetadataPath(metaName);
                Path dest = toStoredMetadataPath(metaName);
                if(_fs.exists(dest)) {
                    throw new IllegalArgumentException("Metadata collision: " + source.toString() + " -> " + dest.toString());
                }
                _fs.rename(source, dest);
            }
        }
    }

    public void absorb(Bucket p) throws IOException {
        absorb(p, new CopyArgs());
    }

    public void absorb(Bucket p, int renameMode) throws IOException {
        CopyArgs args = new CopyArgs();
        args.renameMode = renameMode;
        absorb(p, args);
    }

    public void absorb(Bucket p, Configuration configuration) throws IOException {
        CopyArgs args = new CopyArgs();
        args.configuration = configuration;
        absorb(p, args);
    }

    public void absorb(Bucket p, CopyArgs args) throws IOException {
        args = new CopyArgs(args);
        if(args.renameMode==null) args.renameMode = RenameMode.ALWAYS_RENAME;
        boolean formatsSame = checkCombineValidity(p, args);

        if(formatsSame && p._fs.getUri().equals(_fs.getUri())) {
            moveAppend(p, args);
        } else {
            copyAppend(p, args);
            //TODO: should we go ahead and clear out the input bucket for consistency?
        }
    }

    public void s3ConsistencyFix() throws IOException {
        for(Path p: getStoredFiles()) {
            try {
                _fs.getFileStatus(p);
            } catch(FileNotFoundException e) {
                LOG.info("Fixing file: " + p);
                _fs.create(p, true).close();
            }
        }
    }

    public void consolidate() throws IOException {
        consolidate(Consolidator.DEFAULT_CONSOLIDATION_SIZE);
    }

    public void consolidate(long maxSize) throws IOException {
        List<String> toCheck = new ArrayList<String>();
        toCheck.add("");
        BucketStructure structure = getSpec().getStructure();
        List<String> consolidatedirs = new ArrayList<String>();
        while(toCheck.size()>0) {
            String dir = toCheck.remove(0);
            List<String> dirComponents = componentsFromRoot(dir);
            if(structure.isValidTarget(dirComponents.toArray(new String[dirComponents.size()]))) {
                consolidatedirs.add(toFullPath(dir));
            } else {
                FileStatus[] contents = listStatus(new Path(toFullPath(dir)));
                for(FileStatus f: contents) {
                    if(!f.isDir()) {
                        if(f.getPath().toString().endsWith(EXTENSION))
                            throw new IllegalStateException(f.getPath().toString() + " is not a dir and breaks the structure of " + getInstanceRoot());
                    } else {
                        String newDir;
                        if(dir.length()==0) newDir = f.getPath().getName();
                        else newDir = dir + "/" + f.getPath().getName();
                        toCheck.add(newDir);
                    }
                }
            }
        }

        Consolidator.consolidate(_fs, _format, new BucketPathLister(false), consolidatedirs, maxSize, EXTENSION);
    }

    @Override
    protected RecordInputStream createInputStream(Path path) throws IOException {
        return _format.getInputStream(_fs, path);
    }

    @Override
    protected RecordOutputStream createOutputStream(Path path) throws IOException {
        return _format.getOutputStream(_fs, path);
    }

    @Override
    protected boolean delete(Path path, boolean recursive) throws IOException {
        return _fs.delete(path, recursive);
    }

    @Override
    protected boolean exists(Path path) throws IOException {
        return _fs.exists(path);
    }

    @Override
    protected boolean rename(Path source, Path dest) throws IOException {
        return _fs.rename(source, dest);
    }

    @Override
    protected boolean mkdirs(Path path) throws IOException {
        return _fs.mkdirs(path);
    }

    @Override
    protected FileStatus[] listStatus(Path path) throws IOException {
        FileStatus[] arr =  _fs.listStatus(path);
        List<FileStatus> ret = new ArrayList<FileStatus>();
        for(FileStatus fs: arr) {
            if(!fs.isDir() || !fs.getPath().getName().startsWith("_")) {
                ret.add(fs);
            }
        }
        return ret.toArray(new FileStatus[ret.size()]);
    }

    protected String toFullPath(String relpath) {
       Path p;
       if(relpath.length()==0) p = new Path(getInstanceRoot());
       else p = new Path(getInstanceRoot(), relpath);
       return p.toString();
    }

    protected List<String> componentsFromRoot(String relpath) {
       String fullpath = toFullPath(relpath);
       List<String> full = Utils.componentize(fullpath);
       List<String> root = Utils.componentize(getRoot());
       return Utils.stripRoot(root, full);
    }

    protected void checkValidStructure(String userfilename) {
        List<String> full = componentsFromRoot(userfilename);
        full.remove(full.size()-1);
        //hack to get around how hadoop does outputs --> _temporary and _attempt*
        while(full.size()>0 && full.get(0).startsWith("_")) {
            full.remove(0);
        }
        if(!getSpec().getStructure().isValidTarget(full.toArray(new String[full.size()]))) {
            throw new IllegalArgumentException(
                    userfilename + " is not valid with the bucket structure " + getSpec().toString() +
                    " --> " + full.toString());
        }
    }

    protected static class BucketPathLister implements PathLister {
        boolean _includeMeta;

        public BucketPathLister() {
            this(true);
        }

        public BucketPathLister(boolean includeMeta) {
            _includeMeta = includeMeta;
        }

        public List<Path> getFiles(FileSystem fs, String path) {
            try {
                Bucket p = new Bucket(fs, path);
                List<Path> ret;
                if(_includeMeta) {
                    ret = p.getStoredFilesAndMetadata();
                } else {
                    ret = p.getStoredFiles();
                }
                return ret;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean isEmpty() throws IOException {
        BucketIterator it = iterator();
        boolean ret = !it.hasNext();
        it.close();
        return ret;
    }

    public BucketIterator iterator() {
        return new BucketIterator();
    }

    public class BucketIterator implements Iterator<T> {

        private List<String> filesleft;
        private TypedRecordInputStream curr = null;
        private T nextRecord;

        public BucketIterator() {
            try {
                filesleft = getUserFileNames();
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
            getNextRecord();
        }

        private void getNextRecord() {
            try {
                while(curr==null || (nextRecord = curr.readObject()) == null) {
                    if(curr!=null) curr.close();
                    if(filesleft.size()==0) break;
                    curr = openRead(filesleft.remove(0));
                }
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean hasNext() {
            return nextRecord != null;
        }

        public T next() {
            T ret = nextRecord;
            getNextRecord();
            return ret;
        }

        public void close() throws IOException {
            if(curr!=null) {
                curr.close();
            }
        }

        public void remove() {
            throw new UnsupportedOperationException("Cannot remove records from a bucket");
        }
    }
}
