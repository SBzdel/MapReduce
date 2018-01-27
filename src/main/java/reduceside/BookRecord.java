package reduceside;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Serhii on 1/26/2018.
 */
public class BookRecord implements Writable {

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

}
