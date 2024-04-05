package csx55.hadoop.QuestionOne;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//uses metadat.txt
public class MapperOne extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text artistID = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        artistID.set(value.toString().split("\\|")[2]);
        context.write(artistID, one);

    }
}

