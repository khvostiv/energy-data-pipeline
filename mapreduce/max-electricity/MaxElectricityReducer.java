// Reconstructed from Phase 2 course-report screenshots (2026).
// Formatting restored; not verified against a running Hadoop installation.

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/* The Reducer receives electricity consumption records from the Mapper.
   It compares every consumption value and tracks the largest, along with
   the corresponding house number and hour. */
public class MaxElectricityReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        float maxEnergy = Float.MIN_VALUE;
        String maxHouse = "";
        String maxHour = "";

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            String house = parts[0];
            String hour = parts[1];
            float energy = Float.parseFloat(parts[2]);

            if (energy > maxEnergy) {
                maxEnergy = energy;
                maxHouse = house;
                maxHour = hour;
            }
        }

        context.write(
            new Text(maxHouse + " " + maxHour),
            new Text(String.valueOf(maxEnergy))
        );
    }
}
