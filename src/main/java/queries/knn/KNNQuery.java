package queries.knn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;

import scala.Serializable;
import helpers.LocationUpdate;

public class KNNQuery implements Serializable{
	private int queryID;
	private int focalXCoord, focalYCoord;
	private int k;
	
	PriorityQueue<LocationUpdate> kNNQueue;  // Priority queue (max-heap).
	HashMap<Long, Integer> currentRanks;  // Records the current rank of each object in the top-k list.
		
	// Retrieves the distance of the farthest object in the current top-k.
	public double getFarthestDistance() {
		LocationUpdate farthest = kNNQueue.peek();
		return Math.sqrt(Math.pow((farthest.getNewLocationXCoord() - this.focalXCoord), 2) +
				                  Math.pow((farthest.getNewLocationYCoord() - this.focalYCoord), 2));
	}
	
	public int getK() {
		return this.k;
	}
	
	public void setK(int k) {
		this.k = k;
	}
	
	public int getQueryID() {
		return this.queryID;
	}
	
	public void setQueryID(int id) {
		this.queryID = id;
	}
	
	public int getFocalXCoord() {
		return this.focalXCoord;
	}
	
	public void setFocalXCoord(int focalXCoord) {
		this.focalXCoord = focalXCoord;
	}
	
	public int getFocalYCoord() {
		return this.focalYCoord;
	}
	
	public void setFocalYCoord(int focalYCoord) {
		this.focalYCoord = focalYCoord;
	}
	public List<Integer> getCurrentRanks(){
		
		return (List<Integer>) currentRanks.values();
	}
	
	public KNNQuery(int id, int focalXCoord, int focalYCoord, int k) {
		Comparator<LocationUpdate> maxHeap = new MaxHeap(focalXCoord, focalYCoord);
		this.kNNQueue = new PriorityQueue<LocationUpdate>(50, maxHeap);
		this.currentRanks = new HashMap<Long, Integer>();
		
		this.queryID = id;
	
		this.focalXCoord = focalXCoord;
		this.focalYCoord = focalYCoord;
		this.k = k;
	}
	
	// Returns a representation of the changes in the top-k list (if any).
	public ArrayList<String> processLocationUpdate(LocationUpdate incomingUpdate) {
		boolean topkMayHaveChanged = false;
		// If the new location update corresponds to an object that is already in the top-k list.
		if (this.currentRanks.containsKey(incomingUpdate.getObjectId())) {
			topkMayHaveChanged = true;
			LocationUpdate toBeUpdatedInHeap = null;
			// Locate that object in the priority queue.
			for (LocationUpdate l : kNNQueue) {
				if (l.getObjectId() == incomingUpdate.getObjectId()) {
					toBeUpdatedInHeap = l;
				}
			}
			// Heapify.
			kNNQueue.remove(toBeUpdatedInHeap);			
			kNNQueue.add(incomingUpdate);
		} else {
			// If the current list is small, i.e., has less than k objects, take that object anyway and add it to the topk list.
			if (currentRanks.size() < this.k) {
				topkMayHaveChanged = true;
				this.kNNQueue.add(incomingUpdate);				
			} else {
				// Calculate the distance corresponding to new location.
				double distanceOfObject = Math.sqrt(Math.pow((incomingUpdate.getNewLocationXCoord() - this.focalXCoord), 2) +
						  								   Math.pow((incomingUpdate.getNewLocationYCoord() - this.focalYCoord), 2));
				// New location is closer than the current farthest.
				if (this.getFarthestDistance() > distanceOfObject) {
					topkMayHaveChanged = true;
					// Remove the farthest.
					this.kNNQueue.remove();
					// Add the new object.
					this.kNNQueue.add(incomingUpdate);
				}
			}
		}
		if (topkMayHaveChanged)
			return getTopkUpdates();
		else
			return new ArrayList<String>();
	}
	
	// Returns a string representation of the updates that happened to the top-k list, e.g., removal of an object, addition of
	// and object, or the change of a rank of an object
	private ArrayList<String> getTopkUpdates() {
		// Calculate the new rank of each object in the top-k list.
		HashMap<Long, Integer> newRanks = new HashMap<Long, Integer>();
		int rank = 1;
		for (LocationUpdate l : this.kNNQueue) {
			newRanks.put(l.getObjectId(), rank);
			rank++;
		}

		ArrayList<String> changes = new ArrayList<String>();
		// Compare the new ranks with the existing (i.e., old) ranks.
		for (Long objectId : newRanks.keySet()) {
			if (!this.currentRanks.containsKey(objectId)) {
				changes.add("+ Object " + objectId + " with Rank " + newRanks.get(objectId) +" for Query " + this.queryID );
			} else if (this.currentRanks.get(objectId) != newRanks.get(objectId)) {
				changes.add("U Object " + objectId + " with Rank " + newRanks.get(objectId) +" for Query " + this.queryID );
			}
		}
		for (Long objectId : this.currentRanks.keySet()) {
			if (!newRanks.containsKey(objectId)) {
				changes.add("- Object " + objectId +" for Query " + this.queryID );
			}
		}
		
		// Finally, update the current ranks to reflect the new ranks.
		this.currentRanks = newRanks;
		// Return the change list as a string. Maybe we need to change this later to something more solid.
		return changes;
	}
	
	// This is an internal class that is used to order the objects according to the Euclidean distance using a priority queue.
	public class MaxHeap implements Comparator<LocationUpdate>, Serializable{
		private int focalXCoord, focalYCoord;
		
		public MaxHeap(int focalXCoord, int focalYCoord) {
			this.focalXCoord = focalXCoord;
			this.focalYCoord = focalYCoord;
		}
		
		@Override
		public int compare(LocationUpdate a, LocationUpdate b) {
			double distanceOfA = Math.sqrt(Math.pow((a.getNewLocationXCoord() - this.focalXCoord), 2) +
															  Math.pow((a.getNewLocationYCoord() - this.focalYCoord), 2));
			
			double distanceOfB = Math.sqrt(Math.pow((b.getNewLocationXCoord() - this.focalXCoord), 2) +
															  Math.pow((b.getNewLocationYCoord() - this.focalYCoord), 2));
			
			if (distanceOfA < distanceOfB)
				return -1;
			if (distanceOfA > distanceOfB)
				return 1;

			return 0;
		}
	}
}
