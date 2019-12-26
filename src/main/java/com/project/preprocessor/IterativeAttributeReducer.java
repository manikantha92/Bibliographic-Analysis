package com.project.preprocessor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public final class IterativeAttributeReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for (final Text val : values) {
            if (val != null) {
                sb.append(val.toString() + "\t");
            }
        }
        String it = context.getConfiguration().get("current");
        result.set(it + "\t" + sb.toString().trim());
        context.write(key, result);
    }

}

