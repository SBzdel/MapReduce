package reduceside;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Serhii on 1/26/2018.
 */
public class BookRecord implements Writable, WritableComparable<BookRecord> {

    public Text bookName = new Text();

    public BookRecord(){}

    public BookRecord(String bookName){
        this.bookName.set(bookName);
    }

    public void write(DataOutput out) throws IOException {
        this.bookName.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.bookName.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BookRecord that = (BookRecord) o;

        return bookName != null ? bookName.equals(that.bookName) : that.bookName == null;
    }

    @Override
    public int hashCode() {
        return bookName != null ? bookName.hashCode() : 0;
    }

    @Override
    public int compareTo(BookRecord o) {
        return this.bookName.compareTo(o.bookName);
    }
}
