package reduceside;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Created by Serhii on 1/26/2018.
 */
public class JoinGenericWritable extends GenericWritable implements WritableComparable<JoinGenericWritable> {

    private static Class<? extends Writable>[] CLASSES = null;

    static {
        CLASSES = (Class<? extends Writable>[]) new Class[] {
                StudentRecord.class,
                BookRecord.class
        };
    }

    public JoinGenericWritable() {}

    public JoinGenericWritable(Writable instance) {
        set(instance);
    }

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }

    @Override
    public int compareTo(JoinGenericWritable o) {
        return this.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        return this.get().equals(((JoinGenericWritable)o).get() );
    }

}
