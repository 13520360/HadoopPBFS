import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * <LongWritable, Text, Text , IntWritable>
 * LongWritable, Text: Input Key, Input Value
 * Text , IntWritable: Output Key, Output Value
 * 
 * Graph: A ---10---> B
 * 		  |---5---> D
 * Input Form: A-0|B-10|D-5
 * A-0: Root Node(Mean the node that we focus currently)
 * B-10 and D-5: Adjacent Nodes of Root Node
 *
 * Input Key: A
 * Input Value: A-0|B-10|D-5
 * Output Key: Node.RootNode
 * Output Value: Node.GetOutputValue() => return form like: A-0|B-10|D-5
 */
public class ParallelBFSMapper 
	extends Mapper<LongWritable, Text, Text , Text>{
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 * 
	 * KEYIN should match with Text
	 * VALUEIN should match with Text
	 * 
	 * Pseudo Code
	 * Map(nid n, Node N)
	 * 	d <- N.distance
	 *  EMIT(nid n, N)
	 *  
	 *  for all node in N.adjacentList
	 *  	EMIT(nid node, node.distance + d)
	 */

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String text  = value.toString();
		
		// Parse text string into StringTokenizer line by line
		// Turn itr into a iterator
		String OPERATORS = "\n";
		StringTokenizer itr = new StringTokenizer(text, OPERATORS, false);
		
		while(itr.hasMoreTokens()) {
			String line = itr.nextToken();
			Node node = new Node();
			node.ParseNodeValueFromString(line);

			//Get distance 
			long distance = node.GetDistance();
			//CurrentNodeDistance = CurrentNode.GetDistance()
			//Emit (key, value)
			context.write(new Text(node.GetName()), new Text(node.GetStringValue()));
			
			//For all adjacent in adjacentList
			for (int i = 0 ; i < node.GetAdjacentList().length; i++) {
				//	adjacent.SetDistance(ajacent.GetDistance() + distance)
				
				node.GetAdjacentList()[i].SetDistance(
						node.GetAdjacentList()[i].GetDistance() + distance);
			
				// 	Emit(adjacent.GetName(),adjacent.GetStringValue 
				context.write(new Text(node.GetAdjacentList()[i].GetName()), 
						new Text(node.GetAdjacentList()[i].GetStringValue()));
			}
		}
	}
}
