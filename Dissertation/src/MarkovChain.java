import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class MarkovChain {

	private Set<String>[] states;
	private int[][] transitionMatrix;
	
	public MarkovChain(Set<String>[] states, int[][] transitionMatrix) {
		this.states = states;
		this.transitionMatrix = transitionMatrix;
	}
	
	private int numberOfPossibleOperations(int row) {
		int counter = 0;
		
		for(int i=0; i<transitionMatrix[row].length; i++) {
			if(transitionMatrix[row][i] != 0) {
				counter++;
			}
		}
		
		return counter;
	}
	
	private int getCurrentStateIndex(Set<String> curState) {
		for(int i=0; i<states.length; i++) {
			if(states[i].equals(curState)) {
				return i;
			}
		}
		return -1;
	}
	
	public Boolean isRepaired(Set<String> curState) {
		int curStateIndex = getCurrentStateIndex(curState);
		
		for(int j=0; j<transitionMatrix[curStateIndex].length; j++) {
			if(transitionMatrix[curStateIndex][j] != 0) {
				return false;
			}
		}
		return true;
	}
	
	//gets the current state of the database, and returns a matrix with the possible operations and the probability for each
	public String[][] getOperationsWithProbabilities(Set<String> curState) {
		int curStateIndex = getCurrentStateIndex(curState);
		
		String[][] operations = new String[numberOfPossibleOperations(curStateIndex)][2]; //getting the number of possible operations from the current state
		int resultsIndex = 0;
		
		for(int j=0; j<transitionMatrix[curStateIndex].length; j++) {
			if(transitionMatrix[curStateIndex][j] != 0) {
				Set<String> tempState = new HashSet<String>();
				tempState = states[curStateIndex];
				tempState.removeAll(states[j]);
				
				operations[resultsIndex][0] = tempState.toString();
				operations[resultsIndex][1] = Integer.toString(transitionMatrix[curStateIndex][j]);
				
				resultsIndex++;
			}
		}
		
		return operations;
	}
}
