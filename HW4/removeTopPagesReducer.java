/*
################################################################################
# Author: Eli Sorey
# Last Edit: 5/10/16
################################################################################
*/

package WikiRank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

public class removeTopPagesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	HashMap<String, Float> pagesToRanks = new HashMap<String, Float>();
    	int round = -1;
    	String pageRankLinksString = null;
        for (org.w3c.dom.Text val : values) {
        	// Check for the * control character indicating this value
        	// is to be used in deciding whether to remove this page this
        	// round. If this is the case, the value has the form
        	// *\trank\tpage
        	if (val.toString().charAt(0) == "*") {
        		String valueString = val.toString();
        		valueTokens = valueString.split("\t");
        		float rank = Float.parseFloat(valueTokens[1]);
        		String page = valueTokens[2];
        		pagesToRanks.put(page, rank);
        	}
        	
        	// Check for the ! control character indicating this value
        	// gives the current round of page removal. In this case,
        	// value is of the form !round.
        	else if (val.toString().charAt(0) == "!") {
        		round = Integer.parseInt(val.toString().substring(1));
        	}
        	
        	// If we haven't seen it yet, value is of the form page\trank\tlinks 
        	// and needs to be passed on for the next round.
        	else if (pageRankLinksString == null){
        		pageRankLinksString = val.toString();
        	}
        	
        	// Some unknown value.
        	else {
        		System.out.println("*****~~~~~~*****removeTopPagesReducer -- unknown value received: " + val.toString());
        		System.out.println("Probably want to debug and try again");
        	}
        }
        
        // Check if this page has the highest page rank of its neighbors.
        float maxRank = -1.0;
        String maxPage = null;
        for (Map.Entry<String, Float> entry : pagesToRanks.entrySet()) {
        	float val = entry.getValue();
        	if (val > maxRank) {
        		maxRank = val;
        		maxPage = entry.getKey();
        	}
        }
        
        // If it does have the highest rank of its neighbors, write the pair (@round, page\t\rank).
        if (maxPage.equals(page)) {
        	context.write(new Text("@" + round.toString(), ), new Text(page + "\t" + rank.toString()));
        }
    }
}
