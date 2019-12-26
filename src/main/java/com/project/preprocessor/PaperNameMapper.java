package com.project.preprocessor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.project.preprocessor.Driver.PAPERNAME;

public final class PaperNameMapper extends Mapper<Object, Text, Text, Text> {

    private final Text keyText = new Text();
    private final Text valueText = new Text();
    private String it = null;

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        final String[] lineArr = value.toString().split("\t");
        String paperId = lineArr[0];
        context.write(new Text(lineArr[0]), new Text(PAPERNAME + "\t" + value.toString().substring(paperId.length()).trim()));
    }
}

