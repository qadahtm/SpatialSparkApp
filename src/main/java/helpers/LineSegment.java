package java.helpers;

public class LineSegment 
{
	public Point Start;
	public Point End;
	public int SegmentID;
	
	public LineSegment(Point start, Point end)
	{
		this.Start = start;
		this.End = end;
	}
	
	public LineSegment(float xStart, float yStart, float xEnd, float yEnd)
	{
		this.SegmentID = 0;
		this.Start = new Point(xStart, yStart);
		this.End = new Point(xEnd, yEnd);
	}
	
	public LineSegment(float xStart, float yStart, float xEnd, float yEnd, int segmentID)
	{
		this.SegmentID = segmentID;
		this.Start = new Point(xStart, yStart);
		this.End = new Point(xEnd, yEnd);
	}
	
	public LineSegment()
	{
		this.Start = null;
		this.End = null;
	}
	
	public boolean contains(Point point)
	{
		boolean pointOnLine = false;
		if(hasSlope())
		{
			//line equation is y = a * x + b
			float a = getSlope();
			float b = Start.Y - a * Start.X;
			float y = a * point.X + b;
			if (Math.abs(point.Y - y) < Epsilon)
			{
				pointOnLine = true; // the line is approximately on the line
			}
		}
		else
		{
			//here x1 = x2
			if(point.X == this.Start.X && 
					((point.Y >= this.Start.Y && point.Y <= this.End.Y) || (point.Y <= this.Start.Y && point.Y >= this.End.Y)))
			{
				pointOnLine = true;
			}
		}
		return pointOnLine;
	}
	
	public boolean hasSlope()
	{
		boolean hasSloop = true;
		if(this.Start.X == this.End.X)
		{
			hasSloop = false;
		}
		return hasSloop;
	}
	
	public float getSlope()
	{
		return (End.Y - Start.Y) / (End.X - Start.X);
	}
	
	public static float Epsilon = 0.05f;
}
