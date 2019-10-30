package org.bimserver.serializers.binarygeometry;

public class LineSegment {

	private int index1;
	private int index2;
	private int originalTriangleIndex;

	public LineSegment(int originalTriangleIndex, int index1, int index2) {
		this.originalTriangleIndex = originalTriangleIndex;
		if (index1 > index2) {
			this.index1 = index2;
			this.index2 = index1;
		} else {
			this.index1 = index1;
			this.index2 = index2;
		}
	}
	
	public int getOriginalTriangleIndex() {
		return originalTriangleIndex;
	}
	
	public int getIndex1() {
		return index1;
	}
	
	public int getIndex2() {
		return index2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + index1;
		result = prime * result + index2;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LineSegment other = (LineSegment) obj;
		if (index1 != other.index1)
			return false;
		if (index2 != other.index2)
			return false;
		return true;
	}
}
