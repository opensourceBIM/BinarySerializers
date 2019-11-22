package org.bimserver.serializers.binarygeometry;

/******************************************************************************
 * Copyright (C) 2009-2019  BIMserver.org
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see {@literal<http://www.gnu.org/licenses/>}.
 *****************************************************************************/

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.bimserver.models.geometry.GeometryPackage;
import org.bimserver.shared.HashMapVirtualObject;

/**
 * @author Ruben de Laat
 *
 *	The GeometrySubBuffers are only used to keep the actual message size low (around 1 MB). This is done because WebSockets did not perform well with bigger messages (client side). On the client all GeometrySubBuffers combined result in 1 buffer object.
 *
 */
public class GeometrySubBuffer {
	private final Map<HashMapVirtualObject, HashMapVirtualObject> mapping = new LinkedHashMap<>();
	private int nrTriangles = 0;

	private int preparedByteSize = 24;
	private int nrIndices;
	private int nrLineIndices;
	private int nrVertices;
	private int nrColors;
	private int nrObjects;
	private int totalColorPackSize;
	private GeometryBuffer geometryBuffer;
	private int baseIndex;

	public GeometrySubBuffer(GeometryBuffer geometryBuffer, int baseIndex) {
		this.geometryBuffer = geometryBuffer;
		this.baseIndex = baseIndex;
	}
	
	public int getNrTriangles() {
		return nrTriangles;
	}

	public void put(HashMapVirtualObject hashMapVirtualObject, HashMapVirtualObject data) {
		mapping.put(hashMapVirtualObject, data);
		nrTriangles += (int)(data.eGet(GeometryPackage.eINSTANCE.getGeometryData_NrIndices())) / 3;
	}

	public Set<HashMapVirtualObject> keySet() {
		return mapping.keySet();
	}

	public HashMapVirtualObject get(HashMapVirtualObject info) {
		return mapping.get(info);
	}
	
	public int getPreparedByteSize() {
		return preparedByteSize;
	}

	public void setPreparedByteSize(int preparedByteSize) {
		this.preparedByteSize = preparedByteSize;
	}

	public int getNrIndices() {
		return nrIndices;
	}
	
	public int getNrLineIndices() {
		return nrLineIndices;
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

	public void incNrIndices(int nrIndices) {
		this.nrIndices += nrIndices;
		this.geometryBuffer.incNrIndices(nrIndices);
	}

	public void incNrLineIndices(int nrLineIndices) {
		this.nrLineIndices += nrLineIndices;
		this.geometryBuffer.incNrLineIndices(nrLineIndices);
	}

	public void incNrVertices(int nrVertices) {
		this.nrVertices += nrVertices;
		this.geometryBuffer.incNrVertices(nrVertices);
	}

	public void incNrColors(int nrColors) {
		this.nrColors += nrColors;
		this.geometryBuffer.incNrColors(nrColors);
	}

	public void incPreparedSize(int byteSize) {
		this.preparedByteSize += byteSize;
		this.geometryBuffer.incPreparedByteSize(byteSize);
	}

	public void incNrObjects() {
		this.nrObjects++;
		this.geometryBuffer.incNrObjects();
	}
	
	public int getNrObjects() {
		return nrObjects;
	}

	public void incTotalColorPackSize(int bytes) {
		this.totalColorPackSize += bytes;
	}
	
	public int getTotalColorPackSize() {
		return totalColorPackSize;
	}
	
	public void setBaseIndex(int baseIndex) {
		this.baseIndex = baseIndex;
	}

	public int getBaseIndex() {
		return baseIndex;
	}
}