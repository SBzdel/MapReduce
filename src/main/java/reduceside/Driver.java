package reduceside;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Serhii on 1/26/2018.
 */
public class Driver extends Configured implements Tool {

    public static class JoinGroupingComparator extends WritableComparator {
        public JoinGroupingComparator() {
            super (Key.class, true);
        }

        @Override
        public int compare (WritableComparable a, WritableComparable b){
            Key first = (Key) a;
            Key second = (Key) b;

            return first.studentId.compareTo(second.studentId);
        }
    }

    public static class JoinSortingComparator extends WritableComparator {
        public JoinSortingComparator() {
            super (Key.class, true);
        }

        @Override
        public int compare (WritableComparable a, WritableComparable b){
            Key first = (Key) a;
            Key second = (Key) b;

            return first.compareTo(second);
        }
    }

    public static class BookMapper extends Mapper<LongWritable, Text, Key, JoinGenericWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] recordFields = value.toString().split(",");
            int studentId = Integer.parseInt(recordFields[2]);
            String name = recordFields[1];

            Key recordKey = new Key(studentId, Key.BOOK_RECORD);
            BookRecord record = new BookRecord(name);

            JoinGenericWritable genericRecord = new JoinGenericWritable(record);
            context.write(recordKey, genericRecord);
        }
    }

    public static class StudentMapper extends Mapper<LongWritable, Text, Key, JoinGenericWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] recordFields = value.toString().split(",");
            int studentId = Integer.parseInt(recordFields[0]);
            String firstName = recordFields[1];
            String lastName = recordFields[2];
            String age = recordFields[3];

            Key recordKey = new Key(studentId, Key.STUDENT_RECORD);
            StudentRecord record = new StudentRecord(firstName, lastName, age);
            JoinGenericWritable genericRecord = new JoinGenericWritable(record);
            context.write(recordKey, genericRecord);
        }
    }

    public static class JoinReducer extends Reducer<Key, JoinGenericWritable, NullWritable, Text> {
        public void reduce(Key key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
            StringBuilder studentOutput = new StringBuilder();
            StringBuilder bookOutput = new StringBuilder();

            for (JoinGenericWritable v : values) {
                Writable record = v.get();
                if (key.recordType.equals(Key.STUDENT_RECORD)){
                    StudentRecord sRecord = (StudentRecord) record;
                    studentOutput.append(Integer.parseInt(key.studentId.toString())).append(", ");
                    studentOutput.append(sRecord.studentFirstName.toString()).append(", ");
                    studentOutput.append(sRecord.studentLastName.toString()).append(", ");
                    studentOutput.append(sRecord.studentAge.toString());
                } else {
                    BookRecord bRecord = (BookRecord) record;
                    bookOutput.append(bRecord.bookName.toString());
                }
            }

            context.write(NullWritable.get(), new Text(studentOutput.toString() + " - " + bookOutput.toString()));
        }
    }

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Driver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);

        MultipleInputs.addInputPath(job, new Path("hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/booksData.csv"), TextInputFormat.class, BookMapper.class);
        MultipleInputs.addInputPath(job, new Path("hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/studentsData.csv"), TextInputFormat.class, StudentMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setSortComparatorClass(JoinSortingComparator.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/reduceSideJoinOutput"));
        boolean status = job.waitForCompletion(true);
        if (status) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        int res = ToolRunner.run(new Driver(), args);
    }
}
