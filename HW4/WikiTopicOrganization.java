/*
################################################################################
# Author: Eli Sorey
# Last Edit: 5/10/16
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

public class WikiTopicOrganzation extends Configured implements Tool {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WikiTopicOrganzation(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        for (int i = 0; i < 10; i++) {
            computePageRank(2);
            removeTopPages();

        }
    }
    
    public int computePageRank(int num_iters, String outPath) throws Exception {
        System.out.println("************** COMPUTING PAGERANK *******************");
        NumberFormat nf = new DecimalFormat("00");
    	boolean isCompleted = true; 

        String lastResultPath = null;

        for (int runs = 0; runs < num_iters - 1; runs++) {
	    System.out.println("************** Starting run " + runs +" of " + num_iters + "... *******************");
            String inPath = "wiki/ranking/iter" + nf.format(runs);
            lastResultPath = "wiki/ranking/iter" + nf.format(runs + 1);

            isCompleted = runRankCalculation(inPath, lastResultPath);

            if (!isCompleted) return 1;
        }

        String inPath = "wiki/ranking/iter" + nf.format(runs);

        isCompleted = runRankCalculation(inPath, outPath);

        return 0;
    }


    public boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        Job xmlParser = Job.getInstance(conf, "xmlParser");
        xmlParser.setJarByClass(WikiTopicOrganzation.class);

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

    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(WikiTopicOrganzation.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(RankCalculateMapper.class);
        rankCalculator.setReducerClass(RankCalculateReducer.class);

        return rankCalculator.waitForCompletion(true);
    }

    private boolean runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "rankOrdering");
        rankOrdering.setJarByClass(WikiTopicOrganzation.class);

        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);

        rankOrdering.setMapperClass(RankingMapper.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(TextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

        return rankOrdering.waitForCompletion(true);
    }
    
    private boolean removeTopPages(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "removeTopPages");
        removeTopPages.setJarByClass(WikiTopicOrganzation.class);

        removeTopPages.setOutputKeyClass(Text.class);
        removeTopPages.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(removeTopPages, new Path(inputPath));
        FileOutputFormat.setOutputPath(removeTopPages, new Path(outputPath));

        removeTopPages.setMapperClass(removeTopPagesMapper.class);
        removeTopPages.setReducerClass(removeTopPagesReducer.class);

        return removeTopPages.waitForCompletion(true);
    }

}
