package com.project.preprocessor;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public final class AttributeMergingMapper extends Mapper<Object, Text, Text, Text> {
    private final Text keyText = new Text();
    private final Text valueText = new Text();

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        final String[] lineArr = value.toString().split("\t");
        if (lineArr.length >= 2) {
            keyText.set(lineArr[0]);
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i < lineArr.length; i++) {
                if (lineArr[i] != null || !lineArr[i].trim().equals("")) {
                    sb.append(lineArr[i] + "\t");
                }
            }
            valueText.set(sb.toString().trim());
            context.write(keyText, valueText);
        }
    }
}
