package com.naveen.mapper;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MatrixProductMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context){
		
		String [] strInput = new String[2];
		
		strInput = value.toString().split("\t");
		
		//System.out.println("Str Input first part:" + strInput[0]);
		//System.out.println("Str Input second part:" + strInput[1]);
		//Spiltting the Value clause into Co-Occurrence Vector and Item Vector
		
		String [] inputVector = new String [2];
		
		inputVector = strInput[1].split("@");
		
		//System.out.println("Input Vector first part:" + inputVector[0]);
		//System.out.println("Input Vector second part:" + inputVector[1]);
		
		String [] userVector = inputVector[0].substring(2, inputVector[0].length()-1).split(";");
		
		String [] itemVector = inputVector[1].substring(2, inputVector[1].length()-1).split(";");
		
		//System.out.println("User Vector print:" + Arrays.toString(userVector));
		
		//System.out.println("Item Vector print:" + Arrays.toString(itemVector));
		
		
		for(int i=0; i<userVector.length; i++){
			String [] userRating = userVector[i].split(":");
			
			//System.out.println("user Rating array:" + Arrays.toString(userRating));
			
			
			for(int j=0; j<itemVector.length; j++){
				String [] itemCooc = itemVector[j].split(":");
				
				//System.out.println("Item Cooc vector array:" + Arrays.toString(itemCooc));
				StringBuilder str = new StringBuilder(itemCooc[0]);
				str.append(":");
				str.append(Integer.parseInt(userRating[1])*Integer.parseInt(itemCooc[1]));
				
				//System.out.println("String value at the end:" + str.toString());
				try {
					context.write(new Text(userRating[0]), new Text(str.toString()));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
