package csx55.hadoop.CombineData;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//deals with analysis.txt
public class MapperOne extends Mapper<Object, Text, Text, Text> {

    private final static Text songID = new Text(); //key

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] attributes = value.toString().split("\\|", 2); //first element is the song id, the second element is everything else

        songID.set(attributes[0]);

        String songValue = "A|" + attributes[1];

        context.write(songID, new Text(songValue));

    }
}

