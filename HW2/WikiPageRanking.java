/*
################################################################################
# Author: Aakash Indurkhya
# Last Edit: 1/27/16
# Summary: This is the main class for the WikiRank project that handles all of 
# the different Hadoop jobs necessary. The code for the actual jobs is in the 
# corresponding folder. 
################################################################################
*/


package WikiRank;

// import WikiRank.job2.calculate.RankCalculateMapper;
// import WikiRank.job2.calculate.RankCalculateReduce;
// import WikiRank.job3.result.RankingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class WikiPageRanking {
 
    public static void main(String[] args) throws Exception {
        WikiPageRanking pageRanking = new WikiPageRanking();
 
        //In and Out dirs in HDFS
        pageRanking.runXmlParsing("wiki/in", "wiki/ranking/iter00");
    }
 
    public boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        Job xmlParser = Job.getInstance(conf, "xmlParser");
        xmlParser.setJarByClass(WikiPageRanking.class);

        // Input / Mapper
        FileInputFormat.addInputPath(xmlParser, new Path(inputPath));
        xmlParser.setInputFormatClass(XmlInputFormat.class);
        xmlParser.setMapperClass(WikiPageLinksMapper.class);
        xmlParser.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(xmlParser, new Path(outputPath));
        xmlParser.setOutputFormatClass(TextOutputFormat.class);

        xmlParser.setOutputKeyClass(Text.class);
        xmlParser.setOutputValueClass(Text.class);
        xmlParser.setReducerClass(WikiLinksReducer.class);

        return xmlParser.waitForCompletion(true);
    }
}