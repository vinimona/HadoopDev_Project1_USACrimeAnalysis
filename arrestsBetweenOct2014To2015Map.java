package USA_CRIME_ANALYSIS;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class arrestsBetweenOct2014To2015Map extends Mapper 
									<LongWritable, Text, Text, IntWritable> 
{
	//@Overiding abstract method map() from Mapper class
	public void map(LongWritable keyIn, Text valueIn, Context con) 
						throws IOException, InterruptedException
	{
		StringTokenizer st = new StringTokenizer(valueIn.toString().replaceAll(",,", ",NULL,"), ",");
		int totalTokens = st.countTokens();
		String tokens[] = new String[totalTokens];
		String time_frame = new String("Arrests from October 2014 to 2015");
		boolean arrest = false, within_frame = false;
		int month = 0; 
		
		for (int i = 0; i < totalTokens; i++)
		{
			tokens[i] = st.nextToken();
			if (i == 2)
			{
				String date = tokens[i];
				StringTokenizer st_date = new StringTokenizer(date , "/");
				month = new Integer(st_date.nextToken());
			}
			if (i == 8 && tokens[i].equalsIgnoreCase("true"))
				arrest = true;
			if (i == 17 && (tokens[i].equals("2014") && month >= 10) || 
						   (tokens[i].equals("2015") && month <= 10))
				within_frame = true;
			if (st.countTokens() == 0)
				break;
		}
		
		if (arrest && within_frame)
			con.write(new Text(time_frame), new IntWritable(1));
	}
}


// tokens[2] => field "Date" in the input data
// tokens[8] => field "arrest" in the input data
// tokens[17] => field "year" in the input data
