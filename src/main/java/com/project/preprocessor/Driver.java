package com.project.preprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

//reference: http://www.ccs.neu.edu/home/mirek/code/ReplicatedJoinDriver.java

public class Driver extends Configured implements Tool {

    public static final String AUTHOR = "author";
    public static final String PAPER = "paper";
    public static final String TERM = "term";
    public static final String CONF = "conf";
    public static final String PAPERNAME = "paperName";
    public static final String CITATIONCOUNTER = "citationCounter";
    private static final Logger logger = LogManager.getLogger(com.project.preprocessor.Driver.class);

    public static void main(final String[] args) {

        if (args.length != 5) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new com.project.preprocessor.Driver(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        int completion = -10;
        Boolean computeIndividualDataSets = true;
        Boolean copyPageNames = true;
        Boolean computeCitationCounts = true;
        Boolean computeFinalDataSet = true;
        if (computeIndividualDataSets) {
            String[] iterators = new String[]{AUTHOR, TERM, PAPER, CONF};
            int iterator = 0;
            while (iterator < iterators.length) {
                String it = iterators[iterator];
                System.out.println("---------------------------------------------------------------------");
                System.out.println("Running job for " + it);
                System.out.println("---------------------------------------------------------------------");
                final Configuration conf = getConf();
                final Job job = Job.getInstance(conf, "Filtering DataSet Paper IDs");
                job.setJarByClass(com.project.preprocessor.Driver.class);
                final Configuration jobConf = job.getConfiguration();
                jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
                job.setMapperClass(IterativeAttributePreprocessor.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.getConfiguration().set("current", it);
                FileInputFormat.addInputPath(job, new Path(args[0]));//nodes
                FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + it));//pageIds
                job.waitForCompletion(true);

                final Configuration conf2 = getConf();
                final Job job2 = Job.getInstance(conf2, "Building Mapper for Paper -> " + it);
                job2.setJarByClass(com.project.preprocessor.Driver.class);
                final Configuration jobConf2 = job2.getConfiguration();
                jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");
                //job2.getConfiguration().set("join", "inner");
                job2.setMapperClass(IterativeAttributeMapper.class);
                job2.getConfiguration().set("current", it);
                job2.setReducerClass(IterativeAttributeReducer.class);
                job2.addCacheFile(new Path(args[1] + "/" + it + "/" + "/part-r-00000").toUri());//pageIds
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job2, new Path(args[2]));//edges
                FileOutputFormat.setOutputPath(job2, new Path(args[3] + "/" + it));//final Outputs
                job2.waitForCompletion(true);
                iterator++;
            }
        }
        if (copyPageNames) {
            System.out.println("---------------------------------------------------------------------");
            System.out.println("Running job for copying PageNames to Final DataSet Folder");
            System.out.println("---------------------------------------------------------------------");
            final Configuration conf3 = getConf();
            final Job job3 = Job.getInstance(conf3, "Building Mapper for Paper id-> Paper names ");
            job3.setJarByClass(com.project.preprocessor.Driver.class);
            final Configuration jobConf3 = job3.getConfiguration();
            jobConf3.set("mapreduce.output.textoutputformat.separator", "\t");
            job3.setMapperClass(PaperNameMapper.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job3, new Path(args[1] + "/" + PAPER + "/"));//edges
            FileOutputFormat.setOutputPath(job3, new Path(args[3] + "/" + PAPERNAME));//final Outputs
            completion = job3.waitForCompletion(true) ? 0 : 1;
        }
        if (computeCitationCounts) {
            final Configuration conf4 = getConf();
            final Job job4 = Job.getInstance(conf4, "Most Cited paper");
            job4.setJarByClass(com.project.preprocessor.Driver.class);
            final Configuration jobConf4 = job4.getConfiguration();
            jobConf4.set("mapreduce.output.textoutputformat.separator", "\t");
            job4.setMapperClass(CitedMapper.class);
            job4.setReducerClass(CitedReducer.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(IntWritable.class);
            FileInputFormat.setInputDirRecursive(job4, true);
            FileInputFormat.addInputPath(job4, new Path(args[3] + "/" + PAPER + "/"));//nodes
            FileOutputFormat.setOutputPath(job4, new Path(args[3] + "/" + CITATIONCOUNTER + "/"));//pageIds
            completion = job4.waitForCompletion(true) ? 0 : 1;
            long max = job4.getCounters().findCounter(
                    CitedReducer.Counter.MAX).getValue();
            long maxPaperID = job4.getCounters().findCounter(
                    CitedReducer.Counter.MAXPAPERID).getValue();
            System.out.println("---------------------------------------------------------------------");
            System.out.println("Maximum Cited Paper is with paperId -> " + maxPaperID + " maxValue -> " + max);
            System.out.println("---------------------------------------------------------------------");
        }
        if (computeFinalDataSet) {
            System.out.println("---------------------------------------------------------------------");
            System.out.println("Running job for merging all the individual datasets");
            System.out.println("---------------------------------------------------------------------");
            final Configuration conf4 = getConf();
            final Job job4 = Job.getInstance(conf4, "Final DataSets Merger");
            job4.setJarByClass(com.project.preprocessor.Driver.class);
            final Configuration jobConf4 = job4.getConfiguration();
            jobConf4.set("mapreduce.output.textoutputformat.separator", "\t");
            job4.setMapperClass(AttributeMergingMapper.class);
            job4.setReducerClass(AttributeMergingReducer.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);
            FileInputFormat.setInputDirRecursive(job4, true);
            FileInputFormat.addInputPath(job4, new Path(args[3]));//nodes
            FileOutputFormat.setOutputPath(job4, new Path(args[4]));//pageIds
            completion = job4.waitForCompletion(true) ? 0 : 1;
            long numOfNodes = job4.getCounters().findCounter(
                    AttributeMergingReducer.Counter.TOTAL_COMPLETE_DATASET).getValue();
            long numNodeswthoutAdjList = job4.getCounters().findCounter(
                    AttributeMergingReducer.Counter.DATASET_WITHOUT_CITATIONS).getValue();
            System.out.println("---------------------------------------------------------------------");
            System.out.println("Total number of complete rows with all the data     -> " + numOfNodes);
            System.out.println("Total number of rows with all data except citations -> " + numNodeswthoutAdjList);
            System.out.println("---------------------------------------------------------------------");
        }
        /**
         *The final dataset would be enriched dataset with the following fileds, separated by |
         * 1. PageId, the key for entire data
         * 2. Value, it contains multiple entities, to get each of them, split by "|"
         *  PaperName | Year | Ids of Paper, given paper cites | AjacencyListCount
         * | numberOfPapers where this paper is cited | authorId:authorName:authorsPublicationCount
         * | conferenceId:ConferenceName:conferencePublicationsCount | list of(keywordId:keyword)
         */


        return completion;
    }

}
