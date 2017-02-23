/*
################################################################################
# Author: Aakash Indurkhya
# Last Edit: 2/3/16
# Summary: This mapper prepares the reducer to compute the updated rank scores. 
# For each page, there are three possible types of values. 
# 1) "!" marks that the page is valid and has links
# 2) "givenPage\trank_givenPage\t#Links_givenPage" provides the variables for 
#              rank calculation. 
# 3) "| link1, link2, link3, ...." so that the reducer can recall the outbound
#              links for the given page. 
################################################################################
*/

package WikiRank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int pageTabIndex = value.find("\t");
        int rankTabIndex = value.find("\t", pageTabIndex+1);

        String page = Text.decode(value.getBytes(), 0, pageTabIndex);
        String pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex+1);
        
        // Mark page as an Existing page (ignore red wiki-links) 
        context.write(new Text(page), new Text("!"));

        // Skip pages with no links.
        if(rankTabIndex == -1) return;
        
        // TODO: set up the context for the reducer here. First, extract the necessary text. 
        // Then just write the value explained in the assignment.
        //String linkedPages = Text.decode(value.getBytes(), rankTabIndex+1, value.getLength() - pageWithRank.length());
	String valueString = value.toString();
	String linkedPages = valueString.split("\t")[2];
        String[] linkedPagesList = linkedPages.split(", ");
        int numLinkedPages = linkedPagesList.length;
        Text valToWrite = new Text(pageWithRank + Integer.toString(numLinkedPages));
/*	System.out.println("~~~~ Mapper input: " + value.toString() + "~~~~");
	System.out.println("~~~~ pageWithRank: " + pageWithRank + "~~~~");
	System.out.println("~~~~ Mapper output: " + valToWrite.toString() + "~~~~"); */
        for (String linkedPage : linkedPagesList) {
        	context.write(new Text(linkedPage), valToWrite);
        }
        
        // Also need to pass along the links on the given page.
        // Put the original links of the page for the reduce output. 
        // key: page, value: "|" + links
        context.write(new Text(page), new Text("|" + linkedPages));
        
    }
}
