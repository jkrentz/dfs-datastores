package ca.gristle.hadoop.bucket;

import java.io.Serializable;
import java.util.List;

/**
 * Shouldn't take any args
 */
public interface BucketStructure<T> extends Serializable {
    public boolean isValidTarget(String... dirs);
    public T deserialize(byte[] serialized);
    public byte[] serialize(T object);
    public List<String> getTarget(T object);
    public Class getType();
}
