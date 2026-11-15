// Reconstructed from Phase 2 course-report screenshots (2026).
// Formatting restored; not verified against a running Hadoop installation.

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* The Main class configures and runs the MapReduce job. It specifies the Mapper
   and Reducer classes, sets the input and output paths, enables recursive
   reading of input folders, and starts job execution. */
public class MaxElectricityMain {
    public static void main(String[] args) throws Exception {
        new MaxElectricityMain().run(args);
    }

    public void run(String[] args) throws Exception {
        Path inpath = new Path(args[0]);
        Path outpath = new Path(args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MaxElectricityMain.class);
        job.setJobName("Max Electricity");
        job.setMapperClass(MaxElectricityMapper.class);
        job.setReducerClass(MaxElectricityReducer.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, inpath);
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outpath);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
