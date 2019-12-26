package PageRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;


public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
    /*damping factor set to 0.85*/
    public static final double DAMPING_FACTOR = 0.85;
    public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
    private Logger logger = Logger.getLogger(PageRankReducer.class);
    private int numberOfNodesInGraph;
    private Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numberOfNodesInGraph = context.getConfiguration().getInt(CONF_NUM_NODES_GRAPH, 0);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double summedPageRanks = 0;
        Node originalNode = new Node();

        for (Text textValue : values) {

            Node node = Node.fromMR(textValue.toString());

            if (node.containsAdjacentNodes()) {
                // the source paper
                originalNode = node;
            } else {

                context.getCounter(Counter.DANGLING_RANK).increment((int) (node.getPageRank()
                        * CONVERGENCE_SCALING_FACTOR * CONVERGENCE_SCALING_FACTOR));
                //System.out.println("counter value si " +context.getCounter(Counter.DANGLING_RANK).getValue());
                summedPageRanks += node.getPageRank();

            }
        }


			/*formula to calculate the page rank by dividing
			the damping factor across all papers*/

        double dampingFactor = ((1.0 - DAMPING_FACTOR) / (double) numberOfNodesInGraph);

        double danglingPageRank = context.getCounter(Counter.DANGLING_RANK).getValue() / numberOfNodesInGraph;

        double newPageRank = dampingFactor + (DAMPING_FACTOR * (summedPageRanks +
                (danglingPageRank / CONVERGENCE_SCALING_FACTOR * CONVERGENCE_SCALING_FACTOR)));

        double delta = originalNode.getPageRank() - newPageRank;

        int scaledPR = (int) (newPageRank
                * CONVERGENCE_SCALING_FACTOR * CONVERGENCE_SCALING_FACTOR);

        originalNode.setPageRank(newPageRank);
        if (context.getCounter(Counter.MAX_RANK).getValue() < scaledPR) {
            context.getCounter(Counter.MAX_RANK).setValue(scaledPR);
            context.getCounter(Counter.MAX_NODE).setValue(Long.parseLong(key.toString()));
        }

        outValue.set(originalNode.toString());

        context.write(key, outValue);
        int scaledDelta = Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));
        context.getCounter(Counter.CONV_DELTAS).increment(scaledDelta);
        context.getCounter(Counter.PAGERANK_SUM).increment(Math.abs((int) (newPageRank * CONVERGENCE_SCALING_FACTOR * CONVERGENCE_SCALING_FACTOR)));
        context.getCounter(Counter.NUM_OF_NODES).increment(1);
        context.getCounter(Counter.DANGLING_RANK).setValue(0);
    }

    public static enum Counter {
        CONV_DELTAS,
        NUM_OF_NODES,
        PAGERANK_SUM,
        DANGLING_RANK,
        MAX_RANK,
        MAX_NODE
    }

}
