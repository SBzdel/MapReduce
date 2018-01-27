package reduceside;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;
import reduceside.BookRecord;

/**
 * Created by Serhii on 1/26/2018.
 */
public class JoinGenericWritable extends GenericWritable {

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
}
