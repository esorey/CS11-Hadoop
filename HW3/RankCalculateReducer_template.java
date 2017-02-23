/*
################################################################################
# Author: Aakash Indurkhya
# Last Edit: 1/27/16
# Summary: The reducer confirms that the page is valid and has links and then 
# computes the new rank for the page in a format that is readable by the mapper.
################################################################################
*/

package WikiRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {

    private static final float damping = 0.85F;

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isExistingWikiPage = false;
        String[] split;
        float sumShareOtherPageRanks = 0;
        String links = "";
        
        // For each otherPage: 
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks
        for (Text value : values){
        	
        	// Check for "!" control character, indicating that
        	// we have an exisiting wiki page
        	if (value.toString().equals("!")) {
        		isExistingWikiPage = true;
        	}
        	
        	// Check for a leading '|' character, indicating
        	// that this value contains all of the links on
        	// this page.
        	if (value.toString().charAt(0) == '|') {
        		links = value.toString().substring(1);
        	}
        	
        	// Otherwise the value corresponds to an incoming link, and
        	// we use it to update the pageRank of this page.
        	else {
        		split = value.toString().split("\t");
        		float rank = Float.parseFloat(split[1]);
        		int numLinks = Integer.parseInt(split[2]);
        		float share = rank / numLinks;
        		sumShareOtherPageRanks += share;
        	}
        }

        if(!isExistingWikiPage) return;
        float newRank = (1 - damping) + (damping * sumShareOtherPages);
        
        context.write(page, new Text(newRank + links));
    }
}