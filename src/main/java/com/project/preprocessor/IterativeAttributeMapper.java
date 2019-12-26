package com.project.preprocessor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class IterativeAttributeMapper extends Mapper<Object, Text, Text, Text> {

    private static final String PAPER = "paper";
    private final Text keyText = new Text();
    private final Text valueText = new Text();
    private Map<String, String> ids = new HashMap<>();
    private Set<String> paperIds = new HashSet<>();
    private String it = null;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
        try {
            it = context.getConfiguration().get("current");
            URI[] cachedMapFiles = context.getCacheFiles();
            if (cachedMapFiles == null || cachedMapFiles.length == 0) {
                throw new RuntimeException(
                        "FollowerMaps is not yet set in DistributedCache");
            }
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            Path filePath = new Path(cachedMapFiles[0].toString());
            BufferedReader rdr = new BufferedReader(
                    new InputStreamReader(fileSystem.open(filePath)));
            String line;
            while ((line = rdr.readLine()) != null) {
                String[] lineArr = line.split("\t");
                if (lineArr.length != 0) {
                    if (it.equals(PAPER)) {
                        paperIds.add(lineArr[0]);

                    } else {
                        ids.put(lineArr[0], lineArr[1] + "," + lineArr[2]);

                    }
                }
            }
            rdr.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        final String[] lineArr = value.toString().split("\t");
        String edgeFrom = lineArr[0];
        String edgeTo = lineArr[1];
        if (it.equals(PAPER) && paperIds.contains(edgeTo)) {
            keyText.set(edgeFrom);
            valueText.set(edgeTo);
            context.write(keyText, valueText);
        } else if (ids.containsKey(edgeTo)) {
            keyText.set(edgeFrom);
            valueText.set(edgeTo + "," + ids.get(edgeTo));
            context.write(keyText, valueText);
        }
    }
}

