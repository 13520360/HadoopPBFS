import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class ParallelBFS extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		int number_iteration = 0;
		// Get input arguments and parse into variabels IN and OUT
		String IN = args[0];
		String OUT = args[1];
		
		String infile = IN; // variable define input file in hadoop job
		String outfile = OUT; // variable define output file in hadoop job
		
		// variables hold name output files, to compare latter in the loop 
		String pre_lastOutFile;
		String lastOutFile;
		
		boolean isdone = false;
		boolean success = false;
		
		while (isdone == false) {
			Job job = new Job(getConf());
			// Assign Driver class
			job.setJarByClass(ParallelBFS.class);
			job.setJobName("ParallelBFS");
			
			job.setMapperClass(ParallelBFSMapper.class);
			job.setReducerClass(ParallelBFSReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(infile));
			FileOutputFormat.setOutputPath(job, new Path(outfile));
		
			infile = outfile + "/part-r-00000";
			outfile = OUT + number_iteration;
			
			success = job.waitForCompletion(true);
			
			//Check Results convergence		
			//Stop when 2 files OUTPUT(pre last and last) are equal
			if (number_iteration > 1) {
				pre_lastOutFile = OUT + (number_iteration - 1) + "/part-r-00000";
				lastOutFile = OUT + number_iteration + "/part-r-00000";
				File file1 = new File(pre_lastOutFile);
				File file2 = new File(lastOutFile);
				boolean isTwoEqual = FileUtils.contentEquals(file1, file2);
				
				//Stop checking
				if (isTwoEqual) {
					//2 files are equal, then it convergence 
					isdone = true;
				}
			}
			
			number_iteration++;
		}
		
		return success ? 0 : 1;
	}
	
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ParallelBFS(), args));
	
	}
}