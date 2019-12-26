package com.project.preprocessor;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public final class IterativeAttributePreprocessor extends Mapper<Object, Text, Text, Text> {
    private final Text keyText = new Text();
    private final Text valueText = new Text();

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        String it = context.getConfiguration().get("current");
        final String[] lineArr = value.toString().split("\t");
        if (lineArr[1].equals(it)) {
            keyText.set(lineArr[0]);
            valueText.set(lineArr[2] + "\t" + lineArr[3]);
            context.write(keyText, valueText);
        }
    }
}