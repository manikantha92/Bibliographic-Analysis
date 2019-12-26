package KNN;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.project.preprocessor.AttributeMergingReducer.*;

// The mapper class accepts an object and text (row identifier and row contents) and outputs
// two MapReduce Writable classes, NullWritable and DoubleString (defined earlier)
public class KnnMapper extends Mapper<Object, Text, NullWritable, DoubleString> {

    public static int minDistance(String word1, String word2) {
        int len1 = word1.length();
        int len2 = word2.length();

        // len1+1, len2+1, because finally return dp[len1][len2]
        int[][] dp = new int[len1 + 1][len2 + 1];

        for (int i = 0; i <= len1; i++) {
            dp[i][0] = i;
        }

        for (int j = 0; j <= len2; j++) {
            dp[0][j] = j;
        }

        //iterate though, and check last char
        for (int i = 0; i < len1; i++) {
            char c1 = word1.charAt(i);
            for (int j = 0; j < len2; j++) {
                char c2 = word2.charAt(j);

                //if last two chars equal
                if (c1 == c2) {
                    //update dp value for +1 length
                    dp[i + 1][j + 1] = dp[i][j];
                } else {
                    int replace = dp[i][j] + 1;
                    int insert = dp[i][j + 1] + 1;
                    int delete = dp[i + 1][j] + 1;

                    int min = replace > insert ? insert : replace;
                    min = delete > min ? min : delete;
                    dp[i + 1][j + 1] = min;
                }
            }
        }

        return dp[len1][len2];
    }

    DoubleString distanceAndModel = new DoubleString();
    TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();

    // Declaring some variables which will be used throughout the mapper
    int K;
    String mainPaperName;
    int mainPaperId;
    double mainPaperYear;
    String mainConference;
    String mainAuthor;

    int maxYear = 2015;
    int minYear = 1945;

    // Takes a string and two double values. Converts string to a double and normalises it to
    // a value in the range supplied to reurn a double between 0.0 and 1.0
    private double normalisedDouble(String n1, double minValue, double maxValue) {
        return (Double.parseDouble(n1) - minValue) / (maxValue - minValue);
    }


    // Takes ten pairs of values (three pairs of doubles and two of strings), finds the difference between the members
    // of each pair (using nominalDistance() for strings) and returns the sum of the squared differences as a double.
    private double totalSquaredDistance(double normalisedName, double normalisedYear,
                                        double adjListValue, double authorComparison, double confereneComparison,
                                        double keyWordsComparison) {
        // The sum of squared distances is used rather than the euclidean distance
        // because taking the square root would not change the order.
        // Status and gender are not squared because they are always 0 or 1.
        return normalisedYear + normalisedName + adjListValue
                + authorComparison + confereneComparison + keyWordsComparison;
    }

    // The @Override annotation causes the compiler to check if a method is actually being overridden
    // (a warning would be produced in case of a typo or incorrectly matched parameters)
    @Override
    // The setup() method is run once at the start of the mapper and is supplied with MapReduce's
    // context object
    protected void setup(Context context) throws IOException, InterruptedException {
        String testCase = context.getConfiguration().get("testcase");
        if (testCase!=null) {
            // Read parameter file using alias established in main()
            StringTokenizer st = new StringTokenizer(testCase, "|");

            // Using the variables declared earlier, values are assigned to K and to the test dataset, S.
            // These values will remain unchanged throughout the mapper
            K = Integer.parseInt(st.nextToken());
            mainPaperId = Integer.parseInt(st.nextToken());
            mainPaperName = st.nextToken();
            mainPaperYear = normalisedDouble(st.nextToken(), minYear, maxYear);
            mainAuthor = st.nextToken();
            mainConference = st.nextToken();
        }
    }

    @Override
    // The map() method is run by MapReduce once for each row supplied as the input data
    /**
     *The final dataset would be enriched dataset with the following fileds, separated by |
     * 1. PageId, the key for entire data
     * 2. Value, it contains multiple entities, to get each of them, split by "|"
     *  PaperName | Year | Ids of Paper, given paper cites | AjacencyListCount
     * | numberOfPapers where this paper is cited | authorId:authorName:authorsPublicationCount
     * | conferenceId:ConferenceName:conferencePublicationsCount | list of(keywordId:keyword)
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Tokenize the input line (presented as 'value' by MapReduce) from the csv file
        // This is the training dataset, R
        String rLine = value.toString();
        StringTokenizer st = new StringTokenizer(rLine, "\t");
        String paperId = st.nextToken();
        String valueStr = st.nextToken();
        String[] valueSt = valueStr.split("\\|");
        if(valueSt.length >= 8) {
            String paperName = valueSt[0];
            String year = valueSt[1];
            String adjList = valueSt[2];
            String authors = valueSt[5];
            String conference = valueSt[6];
            String keywords = valueSt[7];
            double adjListValue = 10d;
            if (!adjList.equals(NOCITATIONS)) {
                adjListValue = computeSummedAdjListDist(mainPaperId, adjList);
            }
            double normalisedName = 10d;
            if (!paperName.equals(NONAME)) {
                normalisedName = computeAllEditDistance(paperName.split(" "), mainPaperName.split(" "));
            }
            double normalisedYear = normalisedDouble(year, minYear, maxYear) - mainPaperYear;
            double authorComparison = 5d;
            if (!authors.equals(NOAUTHOR)) {
                authorComparison = computeAllEditDistance(getNames(mainAuthor), getNames(authors));
            }
            double conferenceComparison = 0d;
            if (!conference.equals(NOVENUE)) {
                conferenceComparison = minDistance(mainConference.split(":")[1], conference.split(":")[1]);
            }
            double keyWordsComparison = 110d;
            if (!keywords.equals(NOTERM)) {
                keyWordsComparison = computeAllEditDistance(paperName.split(" "), getNames(keywords));
            }

            // Using these row specific values and the unchanging S dataset values, calculate a total squared
            // distance between each pair of corresponding values.
            double tDist = totalSquaredDistance(normalisedName, normalisedYear, adjListValue, authorComparison, conferenceComparison, keyWordsComparison);

            // Add the total distance and corresponding car model for this row into the TreeMap with distance
            // as key and model as value.
            KnnMap.put(tDist, keywords);
            // Only K distances are required, so if the TreeMap contains over K entries, remove the last one
            // which will be the highest distance number.
            if (KnnMap.size() > K) {
                KnnMap.remove(KnnMap.lastKey());
            }
        }
    }

    private double computeSummedAdjListDist(int mainPaperId, String adjList) {
        String[] temp = adjList.split(",");
        double result = 100d;
        List<String> tempList = Arrays.asList(temp);
        if (tempList.contains(mainPaperId+"")) {
            result = -25d;
        }
        return result;
    }

    private double computeAllEditDistance(String[] mainNames, String[] names) {
        double sum = 0d;
        for (int i = 0; i < mainNames.length; i++) {
            for (int j = 0; j < names.length; j++) {
                if(mainNames[i].equals(names[j])){
                    return -100d;
                }
                sum += minDistance(mainNames[i], names[j]);
            }
        }
        return sum;
    }

    public String[] getNames(String item) {
        String[] output = item.split(",");
        for (int i = 0; i < output.length; i++) {
            String currItem = output[i];
            output[i] = currItem.split(":")[1];
        }
        return output;
    }

    @Override
    // The cleanup() method is run once after map() has run for every row
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Loop through the K key:values in the TreeMap
        for (Map.Entry<Double, String> entry : KnnMap.entrySet()) {
            Double knnDist = entry.getKey();
            String knnModel = entry.getValue();
            // distanceAndModel is the instance of DoubleString declared aerlier
            distanceAndModel.set(knnDist, knnModel);
            // Write to context a NullWritable as key and distanceAndModel as value
            context.write(NullWritable.get(), distanceAndModel);
        }
    }
}


