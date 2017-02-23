/*
################################################################################
# Author: Aakash Indurkhya
# Last Edit: 1/27/16
# Summary: 
################################################################################
*/

package WikiRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WikiLinksReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // All pages start with a rank score of 1.0
        // we want pagerank to look like: "1.0\tLink1, Link2, Link3, ..."
        // ensure pagerank looks like this. 
    	String pagerank;
    	StringBuilder pagerank_sb = new StringBuilder();
    	
    	pagerank_sb.append("1.0\t");
    	for (Text link : values) {
    		pagerank_sb.append(link.toString() + ", ");
    	}
    	// Remove the extra delimiter and space.
    	pagerank_sb.delete(pagerank_sb.length() - 2, pagerank_sb.length() - 1);
    	pagerank = pagerank_sb.toString();

        context.write(key, new Text(pagerank));
    }
}
