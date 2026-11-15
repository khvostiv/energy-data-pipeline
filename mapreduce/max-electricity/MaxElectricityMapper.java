// Reconstructed from Phase 2 course-report screenshots (2026).
// Formatting restored; not verified against a running Hadoop installation.

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/* The Mapper reads one line of electricity consumption data at a time.
   It extracts the house number, hour, and electricity consumption value, then
   sends them to the Reducer. All records are emitted with the same key so the
   Reducer can determine the maximum electricity consumption. */
public class MaxElectricityMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        try {
            String line = value.toString();
            String[] columns = line.trim().split("\\s+");
            String house = columns[1];
            String time = columns[3];
            String hour = time.substring(0, 2);
            String energy = columns[4];

            context.write(new Text("max"), new Text(house + "," + hour + "," + energy));
        } catch (Exception e) {
            // Original screenshot swallows invalid input records.
        }
    }
}
