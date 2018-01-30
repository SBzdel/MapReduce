import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;
import reduceside.BookRecord;
import reduceside.JoinGenericWritable;
import reduceside.Key;
import reduceside.Driver.JoinReducer;
import reduceside.StudentRecord;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by Serhii on 1/30/2018.
 */
public class ReducerTest {

    @Test
    public void reduceWithBookRecord() throws IOException, InterruptedException {
        new ReduceDriver<Key, JoinGenericWritable, NullWritable, Text>()
            .withReducer(new JoinReducer())
            .withInput(new Key(2, Key.BOOK_RECORD),
                Arrays.asList(new JoinGenericWritable(new BookRecord("Harry Poter 1"))))
            .withOutput(NullWritable.get(), new Text(" - Harry Poter 1"))
            .runTest();
    }

    @Test
    public void reduceWithStudentRecord() throws IOException, InterruptedException {
        new ReduceDriver<Key, JoinGenericWritable, NullWritable, Text>()
            .withReducer(new JoinReducer())
            .withInput(new Key(2, Key.STUDENT_RECORD),
                Arrays.asList(new JoinGenericWritable(new StudentRecord("Serhii", "Bzdel", "22"))))
            .withOutput(NullWritable.get(), new Text("2, Serhii, Bzdel, 22 - "))
            .runTest();
    }

}
