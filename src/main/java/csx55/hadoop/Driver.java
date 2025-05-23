package csx55.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import csx55.hadoop.CombineData.CombineDataMain;
import csx55.hadoop.QuestionEight.QuestionEightMain;
import csx55.hadoop.QuestionFive.QuestionFiveMain;
import csx55.hadoop.QuestionFour.QuestionFourMain;
import csx55.hadoop.QuestionNine.QuestionNineMain;
import csx55.hadoop.QuestionOne.QuestionOneMain;
import csx55.hadoop.QuestionSeven.QuestionSevenMain;
import csx55.hadoop.QuestionSix.QuestionSixMain;
import csx55.hadoop.QuestionTwo.QuestionTwoMain;
import csx55.hadoop.QuestionThree.QuestionThreeMain;
import csx55.hadoop.QuestionTen.QuestionTenMain;


public class Driver {

    private Configuration conf = new Configuration();
    private Job job;

    public Configuration getConfiguration(){
        return conf;
    }

    public Job getJob(){
        return job;
    }

    public void setJob(String jobName){
        try{
            this.job = Job.getInstance(getConfiguration(), jobName);
        } catch(IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception{

        if(args.length != 3){
            System.exit(0); 
        }

        String input = args[1]; //this is the path to where to find the dataset
        String output = args[2]; //this is the path to where to store the output

        Driver mapReduce = new Driver();

        // mapReduce.setJob("CombineDataJob");
        // CombineDataMain combineData = new CombineDataMain(mapReduce.getJob(), input, output);
        // combineData.combine();

        // mapReduce.setJob("QuestionOneJob");
        // QuestionOneMain questionOne = new QuestionOneMain(mapReduce.getJob(), input, output);
        // questionOne.answerQuestion();
        
        // mapReduce.setJob("QuestionTwoJob");
        // QuestionTwoMain questionTwo = new QuestionTwoMain(mapReduce.getJob(), input, output);
        // questionTwo.answerQuestion();

        // mapReduce.setJob("QuestionThreeJob");
        // QuestionThreeMain questionThree = new QuestionThreeMain(mapReduce.getJob(), input, output);
        // questionThree.answerQuestion();

        // mapReduce.setJob("QuestionFourJob");
        // QuestionFourMain questionFour = new QuestionFourMain(mapReduce.getJob(), input, output);
        // questionFour.answerQuestion();

        // mapReduce.setJob("QuestionFiveJob");
        // QuestionFiveMain questionFive = new QuestionFiveMain(mapReduce.getJob(), input, output);
        // questionFive.answerQuestion();

        // mapReduce.setJob("QuestionSixJob");
        // QuestionSixMain questionSix = new QuestionSixMain(mapReduce.getJob(), input, output);
        // questionSix.answerQuestion();

        // mapReduce.setJob("QuestionSevenJob");
        // QuestionSevenMain questionSeven = new QuestionSevenMain(mapReduce.getJob(), input, output);
        // questionSeven.answerQuestion();

        // mapReduce.setJob("QuestionEightJob");
        // QuestionEightMain questionEight = new QuestionEightMain(mapReduce.getJob(), input, output);
        // questionEight.answerQuestion();

        // mapReduce.setJob("QuestioNineJob");
        // QuestionNineMain questionNine = new QuestionNineMain(mapReduce.getJob(), input, output);
        // questionNine.answerQuestion();

        mapReduce.setJob("QuestioTenJob");
        QuestionTenMain questionTen = new QuestionTenMain(mapReduce.getJob(), input, output);
        questionTen.answerQuestion();


        
       
        


    } 

}