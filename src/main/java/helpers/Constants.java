package java.helpers;

public class Constants {
	public static final int generatorSeed =1000;
	public static final int dataGeneratorDelay =10;
	public static final int queryGeneratorDelay =1000;
	
	public static final int numQueries = 100;	
	public static final int numMovingObjects = 100;
	
	public static final int xMaxRange =10000;
	public static final int yMaxRange =10000;
	
	public static final int maxK = 10;  // for kNN queries.
	
	public static final int queryMaxWidth =100;
	public static final int queryMaxHeight =100;
	
	// Object's fields
	public static final String objectIdField = "objectID";
	public static final String objectXCoordField = "xCoord";
	public static final String objectYCoordField = "yCoord";

	public static final String queryIdField = "queryID";
	// Range Query fields
	public static final String queryXMinField = "xMin";
	public static final String queryYMinField = "yMin";
	public static final String queryXMaxField = "xMax";
	public static final String queryYMaxField = "yMax";
	
	// kNN Query fields
	public static final String focalXCoordField = "focalXCoord";
	public static final String focalYCoordField = "focalYCoord";
	public static final String kField = "k";
	
	// Topology constants
	public static final String objectLocationGenerator = "object-location-generator";
	public static final String queryGenerator = "query-generator";
	public static final String rangeFilterBolt = "range-filter";
	public static final String kNNFilterBolt = "kNN-filter";
}
