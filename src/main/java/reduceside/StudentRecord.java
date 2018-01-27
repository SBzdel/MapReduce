package reduceside;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Serhii on 1/26/2018.
 */
public class StudentRecord implements Writable {

    public Text studentFirstName = new Text();
    public Text studentLastName = new Text();
    public Text studentAge = new Text();

    public StudentRecord(){}

    public StudentRecord(String productName, String productNumber, String studentAge){
        this.studentFirstName.set(productName);
        this.studentLastName.set(productNumber);
        this.studentAge.set(studentAge);
    }

    public void write(DataOutput out) throws IOException {
        this.studentFirstName.write(out);
        this.studentLastName.write(out);
        this.studentAge.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.studentFirstName.readFields(in);
        this.studentLastName.readFields(in);
        this.studentAge.readFields(in);
    }
}
