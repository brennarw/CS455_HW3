package csx55.hadoop.QuestionOne;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

//uses metadat.txt
public class MapperOne extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text artistID = new Text();

    //this mapper needs to find each unique artistID and write each instance found to the reducer
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        artistID.set(value.toString().split("\\|")[2]);
        context.write(artistID, one);
        //String[] lines = value.toString().split("\n"); //each line represents one artist
        
        //for(String line : lines){
            // String[] attributes = line.split("\\t");
            // ObjectWritable[] pair = new ObjectWritable[2];
            // pair[0] = new ObjectWritable(attributes[0]);
            // pair[1] = new ObjectWritable(attributes[1]);
            //artist.set({attributes[0], attributes[1]}); //artistID is the third attribute in the line
            //System.out.println(value);
            //String finalValue = key.toString() + "\t" + value.toString();
            //context.write(one, value); //this is what writes each <key, value> pair to the reducer
        //}
    }
}

