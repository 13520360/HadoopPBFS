
public class Node {
	private String name = "";
	private long distance = Integer.MAX_VALUE;
	private Node[] adjacentList = null;
	
	/* 
	 * Function receive string line and parse into Node variable
	 * 
	 * Graph: A ---10---> B
	 * 		  |---5---> D
	 * line input: A	A,0|B,10|D,5
	 * A,0: Root Node(Mean the node that we focus currently)
	 * B,10 and D,5: Adjacent Nodes of Root Node
	 *
	 * this.name = 'A'
	 * this.distance = 0
	 * this.adjacentList = adjacentList
	 * 
	 * adjacentList[0].name = 'B'
	 * adjacentList[0].distance = 10
	 * adjacentList[0].adjacentList = NULL
	 * adjacentList[1].name = '1'
	 * adjacentList[1].distance = 5
	 * adjacentList[1].adjacentList = NULL
	 */
	public void ParseNodeValueFromString(String line) {
		String[] splittedLine = line.split("\t", -1);
		int splittedLineIndex = 0;
		//First time input: A	A,0|B,10|D,5, we need to parse the \tab to get node information
		//Second time input: A,0|B,10|D,5, we don't need to parse the \tab
		if (splittedLine.length > 1) {
			splittedLineIndex = 1;
		} else {
			splittedLineIndex = 0;
		}
		
		String[] splittedArray = splittedLine[splittedLineIndex].split("\\|", -1);
		
		//A-0
		//Parse element value
		String[] nodeValueInformation =  splittedArray[0].split("\\,", -1);
		name = nodeValueInformation[0];

		//Check if it is Integer.MAX_VALUE
		if (nodeValueInformation[1].equals("Integer.MAX_VALUE")) {
			distance = Integer.MAX_VALUE;
		} else {
			distance = Integer.parseInt(nodeValueInformation[1]);
		}
		
		//Allocate size of adjacent node
		adjacentList = new Node[splittedArray.length - 1];
		
		//Parse adjacent list value
		for (int i = 1; i < splittedArray.length; i++) {
			adjacentList[i - 1] = new Node();
			adjacentList[i - 1].ParseNodeValueFromString(splittedArray[i]);
		}
	}
		
	/* 
	 * Function return string with form: A-0|B-10|D-5
	 */
	public String GetStringValue() {
		//TODO: implement function return here
		String returnString;
		if (distance >= Integer.MAX_VALUE) {
			returnString = name + "," + "Integer.MAX_VALUE";
		}else {
			returnString = name + "," + Long.toString(distance);
		}
		
		for (int i = 0; i < adjacentList.length; i++) {			
			if (adjacentList[i].distance == Integer.MAX_VALUE){
				returnString += "|";
				returnString += adjacentList[i].name + "," + "Integer.MAX_VALUE";
			} else {
				returnString += "|";
				returnString += adjacentList[i].name + "," + 
						Long.toString(adjacentList[i].distance);
			}
		}
		
		return returnString;
	}
	
	 
	public boolean HasAdjacent() {
		return (adjacentList.length > 0) ? true : false;
	}
	
	public void SetName(String inName) {
		name = inName;
	}
	
	public String GetName() {
		return name;
	}
	
	public void SetDistance(long inDistance) {
		distance = inDistance;
	}
	
	public long GetDistance() {
		return distance;
	}
	
	public Node[] GetAdjacentList() {
		return adjacentList;
	}
}
