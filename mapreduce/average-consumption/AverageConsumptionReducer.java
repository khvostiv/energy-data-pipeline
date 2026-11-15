// Reconstructed from Phase 2 course-report screenshots (2026).
// Formatting restored; not verified against a running Hadoop installation.

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/* Calculates the arithmetic mean of the readings for a house and date. */
public class AverageConsumptionReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> value, Context context)
            throws IOException, InterruptedException {
        float total_consumption = 0.0f;
        int count = 0;

        for (FloatWritable data : value) {
            total_consumption += data.get();
            count++;
        }

        float average_consumption = total_consumption / count;
        context.write(key, new FloatWritable(average_consumption));
    }
}
