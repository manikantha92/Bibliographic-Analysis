package com.project.preprocessor;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.project.preprocessor.Driver.CITATIONCOUNTER;

public class CitedReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final Text result = new Text();

    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (final IntWritable val : values) {
            sum += val.get();
        }
        if (context.getCounter(Counter.MAX).getValue() < sum) {
            context.getCounter(Counter.MAX).setValue(sum);
            context.getCounter(Counter.MAXPAPERID).setValue(Integer.parseInt(key.toString()));
        }
        result.set(CITATIONCOUNTER + "\t" + sum);
        context.write(key, result);
    }

    public static enum Counter {
        MAX,
        MAXPAPERID
    }

}

