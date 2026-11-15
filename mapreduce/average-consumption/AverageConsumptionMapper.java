// Reconstructed from Phase 2 course-report screenshots (2026).
// Formatting restored; not verified against a running Hadoop installation.

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/* Reads a household electricity record and groups readings by date and house.
   Note: the original MapReduce project averages the input reading values;
   this differs from the Phase 4 Spark calculation using interval deltas. */
public class AverageConsumptionMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            String[] colsData = value.toString().split("\\s+");

            if (colsData[0].equalsIgnoreCase("LOG_ID")) {
                return;
            }

            String house_number = colsData[1];
            String date = colsData[2];
            float electricity_consumption = Float.valueOf(colsData[4]);
            String outputKey = date + "," + house_number;

            context.write(new Text(outputKey), new FloatWritable(electricity_consumption));
        } catch (Exception e) {
            // Original screenshot swallows invalid input records.
        }
    }
}
