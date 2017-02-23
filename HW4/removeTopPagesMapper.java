/*
################################################################################
# Author: Eli Sorey
# Last Edit: 5/10/16
################################################################################
*/

package WikiRank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class removeTopPagesMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    /*
     * map receives a page, rank, round number, and list of linked pages.
     * It outputs the pairs (linkedPage, *\trank\tpage) for each linkedPage,
     * (page, !round), and (page, page\trank\tlinkedPages).
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	/*
    	 * value has the form "!round!page\trank\tlink1, link2, ... 
    	 * or "page\t\rank\tlink1, link2, ....".
    	 * During round 0, there will be no !round!. After that, the round number
    	 * will be given as !round!. 
    	 */
        int round = 0;
        int page_start_ind = 0;
        String valueString = value.toString();
        
        // Check for ! control character indicating that this is not round 0.
        if (valueString.charAt(0) == "!") {
            second_exclam_ind = valueString.substring(1).indexOf("!") + 1;
            round = Integer.parseInt(valueString.substring(0, second_exclam_ind));
            page_start_ind = second_exclam_ind + 1;
        }
        
        // Parse out the relevant parts of the value.
        String cleanValueString = valueString.substring(page_start_ind);
        String[] valueTokens = cleanValueString.split("\t");
        
        String page = valueTokens[0];
        float rank = Float.parseFloat(valueTokens[1]);
        String linkedPages = cleanValueString.split("\t")[2];
        String[] linkedPagesList = linkedPages.split(", ");
        
        
        for (String linkedPage : linkedPagesList) {
        	// Note the * control character.
        	context.write(new Text(linkedPage), new Text("*\t" + rank.toString() + "\t" + page));
        }
        
        context.write(new Text(page), new Text("!" + round.toString()));
        context.write(new Text(page), new Text(page + "\t" + rank.toString() + "\t" + linkedPages));
        
        
    }
}
