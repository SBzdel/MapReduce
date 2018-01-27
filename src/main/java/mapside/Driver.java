package mapside;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Serhii on 1/26/2018.
 */
public class Driver extends Configured implements Tool {

    public static class BookMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private HashMap<Integer, String> students = new HashMap<Integer, String>();
        private void readStudents(List<String> lines) throws IOException{
            StringBuilder studentOutput = new StringBuilder();
            for (String line: lines) {
                String[] recordFields = line.split(",");
                int key = Integer.parseInt(recordFields[0]);
                String firstName = recordFields[1];
                String lastName = recordFields[2];
                String age = recordFields[3];
                String value = firstName + " " + lastName + " " + age;
                students.put(key, value);
            }
        }

        public void setup(Context context) throws IOException{
            URI[] uris = context.getCacheFiles();
            FSDataInputStream dataIn = FileSystem.get(context.getConfiguration()).open(new Path(uris[0]));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(dataIn));

            List<String> lines = new ArrayList<>();
            String line = bufferedReader.readLine();
            while (line != null) {
                lines.add(bufferedReader.readLine());
            }
            readStudents(lines);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] recordFields = value.toString().split(",");
            int studentId = Integer.parseInt(recordFields[2]);
            String bookName = recordFields[1];

            context.write(NullWritable.get(), new Text(bookName + "  " + students.get(studentId)));
        }
    }

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Driver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(BookMapper.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/booksData.csv"));
        job.addCacheFile(new URI("hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/studentsData.csv"));
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path("hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/mapSideJoinOutput"));
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
