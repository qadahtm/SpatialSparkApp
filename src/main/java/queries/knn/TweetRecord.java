package queries.knn;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TweetRecord implements Serializable {
  private int tweetId;
  private String createdAt; //2013-04-01 00:00:00 yyyy-MM SimpleDateFormat 
  private double geoLat;
  private double geoLong;
  private String tweetText;
  private String source;
  private int userId;
  private int coordinated;
  private SimpleDateFormat sdf;

	public TweetRecord(int tweetId, String createdAt, double geoLat, double geoLong, String tweetText, String source, int userId, int coordinated){
		this.tweetId = tweetId;
  		this.createdAt = createdAt;
  		this.geoLat = geoLat;
  		this.geoLong = geoLong;
 		this.tweetText = tweetText;
 		this.source = source;
 		this.userId = userId;
 		this.coordinated = coordinated;
 		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}
	public int getTweetId(){
		return tweetId;
	}

	public String getCreatedAt(){
		return createdAt;
	}
	
	public Date getDateCreatedAt(){
		try {
			return sdf.parse(createdAt);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	public double getGeoLat(){
		return geoLat;
	}

	public double getGeoLong(){
		return geoLong;
	}

	public String getTweetText(){
		return tweetText;
	}
	public String getSource() {
		return source;
	}
	public int getUserId() {
		return userId;
	}
	
	public int getCoordinated() {
		return coordinated;
	}
}
