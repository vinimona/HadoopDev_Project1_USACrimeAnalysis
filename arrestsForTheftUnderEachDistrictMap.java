package USA_CRIME_ANALYSIS;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class arrestsForTheftUnderEachDistrictMap extends Mapper 
									<LongWritable, Text, Text, IntWritable> 
{
	//@Overiding abstract method map() from Mapper class
	public void map(LongWritable keyIn, Text valueIn, Context con) 
						throws IOException, InterruptedException
	{
		StringTokenizer st = new StringTokenizer(valueIn.toString().replaceAll(",,", ",NULL,"), ",");
		int totalTokens = st.countTokens();
		String tokens[] = new String[totalTokens];
		String district = new String("No District");
		boolean theft = false, arrest = false;
		for (int i = 0; i < totalTokens; i++)
		{
			tokens[i] = st.nextToken();
			if (i == 5 && tokens[i].equalsIgnoreCase("THEFT"))
				theft = true;
			if (i == 8 && tokens[i].equalsIgnoreCase("true"))
				arrest = true;
			if (i == 11)
				district = tokens[i];
			if (st.countTokens() == 0)
				break;
		}
		
		if (theft && arrest)
			con.write(new Text(district), new IntWritable(1));
	}
}

//tokens[5] => field "primary type" in the input data
//tokens[8] => field "arrest" in the input data
//tokens[11] => field "district" in the input data