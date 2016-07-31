package USA_CRIME_ANALYSIS;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class casesUnderFBICode32Map extends Mapper <LongWritable, Text, Text, IntWritable> 
{
	//@Overiding abstract method map() from Mapper class
	public void map(LongWritable keyIn, Text valueIn, Context con) 
						throws IOException, InterruptedException
	{
		StringTokenizer st = new StringTokenizer(valueIn.toString().replaceAll(",,", ",NULL,"), ",");
		int totalTokens = st.countTokens();
		String tokens[] = new String[totalTokens];
 		for (int i = 0; i < totalTokens; i++)
		{
			tokens[i] = st.nextToken();
			if (i == 14 && tokens[i].toUpperCase().equalsIgnoreCase("32"))
				con.write(new Text(tokens[i]), new IntWritable(1));
			if (st.countTokens() == 0)
				break;
		}
	}
}

//tokens[14] => FBI code