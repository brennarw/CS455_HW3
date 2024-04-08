package csx55.hadoop.QuestionFour;

import java.io.IOException;

import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//uses both data sets
//value = end_of_fad_in + (duration - start_of_fade_out)
public class MapperOne extends Mapper<Object, Text, Text, FloatWritable> {

    private FloatWritable fadingTime = new FloatWritable();
    private Text artistID = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //key = artistID
        //value = fading time
        
        String[] attributes = value.toString().split("\\|");
        float calculatedFadingTime = Float.parseFloat(attributes[5]) + (Float.parseFloat(attributes[4]) - Float.parseFloat(attributes[12]));

        fadingTime.set(calculatedFadingTime); //end_of_fad_in + (duration - start_of_fade_out)
        artistID.set(attributes[34]); //artistID

        context.write(new Text(artistID), fadingTime);

    }
}

