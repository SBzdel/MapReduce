import org.junit.Test;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import reduceside.*;
import reduceside.Driver.BookMapper;
import reduceside.Driver.StudentMapper;

/**
 * Created by Serhii on 1/30/2018.
 */
public class MapperTest {

    @Test
    public void bookMapper() throws IOException {
        Text value = new Text("2,Harry Poter 1,2");

        new MapDriver<LongWritable, Text, Key, JoinGenericWritable>()
            .withMapper(new BookMapper())
            .withInput(new LongWritable(0), value)
            .withOutput(new Key(2, Key.BOOK_RECORD), new JoinGenericWritable(new BookRecord("Harry Poter 1")))
            .runTest();
    }

    @Test
    public void studentMapper() throws IOException {
        Text value = new Text("2,Serhii,Bzdel,22");

        new MapDriver<LongWritable, Text, Key, JoinGenericWritable>()
            .withMapper(new StudentMapper())
            .withInput(new LongWritable(0), value)
            .withOutput(new Key(2, Key.STUDENT_RECORD), new JoinGenericWritable(new StudentRecord("Serhii", "Bzdel", "22")))
            .runTest();
    }

}
