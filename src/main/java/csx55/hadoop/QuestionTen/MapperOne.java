package csx55.hadoop.QuestionTen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<Object, Text, Text, Text> {

    /*have 9 reducers
    * reducer 1 key: 1926
    * reducer 2 key: 1936
    * reducer 3 key: 1946
    * reducer 4 key: 1956
    * reducer 5 key: 1966
    * reducer 6 key: 1976
    * reducer 7 key: 1986
    * reducer 8 key: 1996
    * reducer 9 key: 2006
    */

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] attributes = value.toString().split("\\|");

        int year = Integer.parseInt(attributes[44]);

        if(year != 0 && year < 1936){
            context.write(new Text("1926"), new Text(attributes[41])); //artist terms
        }
        else if(year != 0 && year < 1946){
            context.write(new Text("1936"), new Text(attributes[41])); //artist terms
        }
        else if(year != 0 && year < 1956){
            context.write(new Text("1946"), new Text(attributes[41])); //artist terms
        }
        else if(year != 0 && year < 1966){
            context.write(new Text("1956"), new Text(attributes[41])); //artist terms
        }
        else if(year != 0 && year < 1976){
            context.write(new Text("1966"), new Text(attributes[41])); //artist terms
        }
        else if(year != 0 && year < 1986){
            context.write(new Text("1976"), new Text(attributes[41])); //artist terms
        }
        else if(year != 0 && year < 1996){
            context.write(new Text("1986"), new Text(attributes[41])); //artist terms
        }
        else if(year != 0 && year < 2006){
            context.write(new Text("1996"), new Text(attributes[41])); //artist terms
        }
        else if(year != 0 && year <= 2010){
            context.write(new Text("2006"), new Text(attributes[41])); //artist terms
        }

        //System.out.println("year:" + attributes[44]);





    }
}

