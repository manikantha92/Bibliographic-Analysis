package KNN;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;
import java.util.*;

// The reducer class accepts the NullWritable and DoubleString objects just supplied to context and
// outputs a NullWritable and a Text object for the final classification.
public class KnnReducer extends Reducer<NullWritable, DoubleString, NullWritable, Text>
{

    TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
    int K;

    @Override
    // The reduce() method accepts the objects the mapper wrote to context: a NullWritable and a DoubleString
    public void reduce(NullWritable key, Iterable<DoubleString> values, Context context) throws IOException, InterruptedException
    {
        String testCase = context.getConfiguration().get("testcase");
        StringTokenizer st = new StringTokenizer(testCase, "\\|");
        K = Integer.parseInt(st.nextToken());
        // values are the K DoubleString objects which the mapper wrote to context
        // Loop through these
        for (DoubleString val : values)
        {
            String rModel = val.getModel();
            double tDist = val.getDistance();

            // Populate another TreeMap with the distance and model information extracted from the
            // DoubleString objects and trim it to size K as before.
            KnnMap.put(tDist, rModel);
            if (KnnMap.size() > K)
            {
                KnnMap.remove(KnnMap.lastKey());
            }
        }

        // This section determines which of the K values (models) in the TreeMap occurs most frequently
        // by means of constructing an intermediate ArrayList and HashMap.

        // A List of all the values in the TreeMap.
        List<String> knnList = new ArrayList<String>(KnnMap.values());

        Map<String, Integer> freqMap = new HashMap<String, Integer>();

        // Add the members of the list to the HashMap as keys and the number of times each occurs
        // (frequency) as values
        for(int i=0; i< knnList.size(); i++)
        {
            Integer frequency = freqMap.get(knnList.get(i));
            if(frequency == null)
            {
                freqMap.put(knnList.get(i), 1);
            } else
            {
                freqMap.put(knnList.get(i), frequency+1);
            }
        }

        // Examine the HashMap to determine which key (model) has the highest value (frequency)
        String mostCommonModel = null;
        int maxFrequency = -1;
        for(Map.Entry<String, Integer> entry: freqMap.entrySet())
        {
            if(entry.getValue() > maxFrequency)
            {
                mostCommonModel = entry.getKey();
                maxFrequency = entry.getValue();
            }
        }

        // Finally write to context another NullWritable as key and the most common model just counted as value.
        context.write(NullWritable.get(), new Text(mostCommonModel)); // Use this line to produce a single classification
//			context.write(NullWritable.get(), new Text(KnnMap.toString()));	// Use this line to see all K nearest neighbours and distances
    }
}
