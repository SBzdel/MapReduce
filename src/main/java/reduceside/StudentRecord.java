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
public class StudentRecord implements Writable, WritableComparable<StudentRecord> {

    public Text studentFirstName = new Text();
    public Text studentLastName = new Text();
    public Text studentAge = new Text();

    public StudentRecord(){}

    public StudentRecord(String firstName, String lastName, String age){
        this.studentFirstName.set(firstName);
        this.studentLastName.set(lastName);
        this.studentAge.set(age);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StudentRecord that = (StudentRecord) o;

        if (studentFirstName != null ? !studentFirstName.equals(that.studentFirstName) : that.studentFirstName != null) return false;
        if (studentLastName != null ? !studentLastName.equals(that.studentLastName) : that.studentLastName != null) return false;
        return studentAge != null ? studentAge.equals(that.studentAge) : that.studentAge == null;
    }

    @Override
    public int hashCode() {
        int result = studentFirstName != null ? studentFirstName.hashCode() : 0;
        result = 31 * result + (studentLastName != null ? studentLastName.hashCode() : 0);
        result = 31 * result + (studentAge != null ? studentAge.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(StudentRecord o) {
        return this.compareTo(o);
    }
}
