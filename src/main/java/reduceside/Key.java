package reduceside;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Serhii on 1/26/2018.
 */
public class Key implements WritableComparable<Key> {

    public static final IntWritable STUDENT_RECORD = new IntWritable(0);
    public static final IntWritable BOOK_RECORD = new IntWritable(1);

    public IntWritable studentId = new IntWritable();
    public IntWritable recordType = new IntWritable();

    public Key(){}
    public Key(int productId, IntWritable recordType) {
        this.studentId.set(productId);
        this.recordType = recordType;
    }

    public void write(DataOutput out) throws IOException {
        this.studentId.write(out);
        this.recordType.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.studentId.readFields(in);
        this.recordType.readFields(in);
    }

    public int compareTo(Key other) {
        if (this.studentId.equals(other.studentId )) {
            return this.recordType.compareTo(other.recordType);
        } else {
            return this.studentId.compareTo(other.studentId);
        }
    }

    public boolean equals (Key other) {
        return this.studentId.equals(other.studentId) && this.recordType.equals(other.recordType );
    }

    public int hashCode() {
        return this.studentId.hashCode();
    }
}
