package org.bimserver.serializers.binarygeometry;

import java.util.LinkedHashMap;
import java.util.Map;

import org.bimserver.shared.HashMapVirtualObject;

public class GeometryBuffer {
	private final Map<HashMapVirtualObject, HashMapVirtualObject> geometryMapping = new LinkedHashMap<>();

	private int preparedByteSize = 20;
	private int nrIndices;
	private int nrVertices;
	private int nrColors;
	private int nrObjects;

	private int totalColorPackSize;

	public int getPreparedByteSize() {
		return preparedByteSize;
	}

	public void setPreparedByteSize(int preparedByteSize) {
		this.preparedByteSize = preparedByteSize;
	}

	public int getNrIndices() {
		return nrIndices;
	}

	public void setNrIndices(int nrIndices) {
		this.nrIndices = nrIndices;
	}

	public int getNrVertices() {
		return nrVertices;
	}

	public void setNrVertices(int nrVertices) {
		this.nrVertices = nrVertices;
	}

	public int getNrColors() {
		return nrColors;
	}

	public void setNrColors(int nrColors) {
		this.nrColors = nrColors;
	}

	public void incNrIndices(int nrIndices2) {
		this.nrIndices += nrIndices2;
	}

	public void incNrVertices(int nrVertices2) {
		this.nrVertices += nrVertices2;
	}

	public void incNrColors(int i) {
		this.nrColors += i;
	}

	public void incPreparedSize(int i) {
		this.preparedByteSize += i;
	}
	
	public Map<HashMapVirtualObject, HashMapVirtualObject> getGeometryMapping() {
		return geometryMapping;
	}

	public void incNrObjects() {
		this.nrObjects++;
	}
	
	public int getNrObjects() {
		return nrObjects;
	}

	public boolean isEmpty() {
		return geometryMapping.isEmpty();
	}

	public void incTotalColorPackSize(int bytes) {
		this.totalColorPackSize += bytes;
	}
	
	public int getTotalColorPackSize() {
		return totalColorPackSize;
	}
}
