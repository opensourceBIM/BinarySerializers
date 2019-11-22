package org.bimserver.serializers.binarygeometry;

import java.util.HashSet;
import java.util.Set;

import org.bimserver.geometry.Vector3D;

public class IndexedVertex3D extends Vector3D {

	private int originalIndex;
	private Set<Integer> referencedFrom = new HashSet<>();

	public IndexedVertex3D(int originalIndex, double x, double y, double z) {
		super(x, y, z);
		this.originalIndex = originalIndex;
	}
	
	public void addReferencedFrom(int index) {
		this.referencedFrom.add(index);
	}
	
	public void setOriginalIndex(int originalIndex) {
		this.originalIndex = originalIndex;
	}
	
	public int getOriginalIndex() {
		return originalIndex;
	}
}
