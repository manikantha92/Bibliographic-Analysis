package KNN;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class KNN {

    public static List<String> createInputFile(Path file)
            throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = file.getFileSystem(conf);
        List<String> TestCases = new ArrayList<>();
        LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
        while (iter.hasNext()) {
            String line = iter.nextLine();
            TestCases.add(line);
        }
        return TestCases;
    }
    // Main program to run: By calling MapReduce's 'job' API it configures and submits the MapReduce job.
    public static void main(String[] args) throws Exception {
        // Create configuration
        Configuration conf = new Configuration();

        if (args.length != 3) {
            System.err.println("Usage: KnnPattern <in> <out> <parameter file>");
            System.exit(2);
        }


        Iterator<String> allT =  createInputFile(new Path(args[2]+"/demo.txt")).iterator();
        int iterator =0;
        int complete =0;
        while (allT.hasNext()) {
            String currTest = allT.next();
            iterator++;
            System.out.println("---------------------------------------------------------------------");
            System.out.println("Running job for total " + currTest.split("//|")[0] + " neighbours.");
            System.out.println("---------------------------------------------------------------------");
            // Create job
            Job job = Job.getInstance(conf, "Find K-Nearest Neighbour");
            job.setJarByClass(KNN.class);
            // Set the third parameter when running the job to be the parameter file and give it an alias
            // Setup MapReduce job
            job.setMapperClass(KnnMapper.class);
            job.setReducerClass(KnnReducer.class);
            job.getConfiguration().set("testcase",currTest);
            job.setNumReduceTasks(1); // Only one reducer in this design

            // Specify key / value
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(DoubleString.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            // Input (the data file) and Output (the resulting classification)
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+iterator+"/"));
            complete = job.waitForCompletion(true) ? 0 : 1;
        }

        // Execute job and return status
        System.exit(complete);
    }
}
