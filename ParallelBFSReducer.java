import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParallelBFSReducer 
	extends Reducer<Text, Text, Text, Text>{

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 * 
	 * KEYIN should match with Text
	 * VALUEIN should match with Text
	 * 
	 * Pseudo code
	 * Reducer(nid m, [d1,d2,...])
	 * 	dmin <- INFINITY
	 * 	M <- null
	 *  for all d in counts [d1,d2,...] do
	 *  	if IsNode(d) then
	 *  		M <- d
	 *  		dmin <- d.distance
	 *  	else if d < dmin then
	 *  		dmin <- d
	 *  M.distance <- dmin
	 *  EMIT(nid m, Node M)
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
	        throws IOException, InterruptedException {
		/*
		 * Pseudo code
		 * Reducer(nid m, [d1,d2,...])
		 * 	dmin <- INFINITY
		 * 	M <- null
		 *  for all d in counts [d1,d2,...] do
		 *  	if IsNode(d) then
		 *  		M <- d
		 *  		dmin <- d.distance
		 *  	else if d < dmin then
		 *  		dmin <- d
		 *  M.distance <- dmin
		 *  EMIT(nid m, Node M)
		 */
		
		// dmin <- INFINITY
		long dmin = Integer.MAX_VALUE;
		// M <- null
		Node M = new Node();
		
		// for all d in counts [d1,d2,...] do
		for (Text value : values) {
			Node node = new Node();
			node.ParseNodeValueFromString(value.toString());
			
			if (node.HasAdjacent()) {
				//if IsNode(d) then
				//M <- d
				//dmin <- d.distance
				M = node;
				if (node.GetDistance() < dmin) {
					dmin = node.GetDistance();
				}
			} else if(node.GetDistance() < dmin) {
				//else if d < dmin then
				//dmin <- d
				dmin = node.GetDistance();
			}
		}
		
		 //M.distance <- dmin
		 //EMIT(nid m, Node M)
		M.SetDistance(dmin);
		context.write(new Text(M.GetName()),new Text(M.GetStringValue()));
	}
}
