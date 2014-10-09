package helpers;

public class LocationUpdate {

	private long objectId;
	private int newLocationXCoord;
	private int newLocationYCoord;
	
	public LocationUpdate(long objectId, int newLocationXCoord, int newLocationYCoord) {
		this.objectId = objectId;
		this.newLocationXCoord = newLocationXCoord;
		this.newLocationYCoord = newLocationYCoord;
	}
	
	public long getObjectId() {
		return this.objectId;
	}
	
	public int getNewLocationXCoord() {
		return this.newLocationXCoord;
	}
	
	public int getNewLocationYCoord() {
		return this.newLocationYCoord;
	}
	
	public void setObjectID(int id) {
		this.objectId = id;
	}
	
	public void setNewLocationXCoord(int newLocationXCoord) {
		this.newLocationXCoord = newLocationXCoord;
	}
	
	public void setNewLocationYCoord(int newLocationYCoord) {
		this.newLocationYCoord = newLocationYCoord;
	}
}
