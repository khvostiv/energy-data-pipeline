// Reconstructed from Phase 2 course-report screenshots (2026).
// Formatting restored; not verified against a running Hadoop installation.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* The input path selects populated year/month text files in HDFS.
   This is a separate job from the maximum electricity analysis. */
public class AverageConsumptionMain {
    public static void main(String[] args) throws Exception {
        new AverageConsumptionMain().run(args);
    }

    public void run(String[] args) throws Exception {
        Path inpath = new Path(args[0] + "/*/*/*.txt");
        Path outpath = new Path(args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(AverageConsumptionMain.class);
        job.setJobName("Average Consumption");
        job.setMapperClass(AverageConsumptionMapper.class);
        job.setReducerClass(AverageConsumptionReducer.class);
        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, inpath);
        FileOutputFormat.setOutputPath(job, outpath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
