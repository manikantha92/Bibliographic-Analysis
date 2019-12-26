package com.project.preprocessor;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.project.preprocessor.Driver.PAPER;

public class CitedMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text keyText = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        final String[] lineArr = value.toString().split("\t");
        if (lineArr.length >= 3) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < lineArr.length; i++) {
                if ((lineArr[i] != null || !lineArr[i].trim().equals("")) && !lineArr[i].equals(PAPER)) {
                    keyText.set(lineArr[i]);
                    context.write(keyText, one);
                }
            }
        }
    }
}
