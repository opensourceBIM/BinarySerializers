package org.bimserver.serializers.binarygeometry;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.bimserver.geometry.Vector3D;

public class Mesh {
	private IntBuffer indices;
	private DoubleBuffer vertices;
	private FloatBuffer normals;
	private Map<Integer, Set<Integer>> referencedVertices = new HashMap<>();
	private Map<Vector3D, Set<IndexedVertex3D>> verticesMap = new HashMap<>();
	
	public Mesh(IntBuffer indices, DoubleBuffer vertices, FloatBuffer normals) {
		this.indices = indices;
		this.vertices = vertices;
		this.normals = normals;
		for (int i=0; i<indices.capacity(); i+=3) {
			for (int j=0; j<3; j++) {
				int index = indices.get(i + j);
				Set<Integer> set = referencedVertices.get(index);
				if (set == null) {
					set = new HashSet<>();
					referencedVertices.put(index, set);
				}
				set.add(i);
			}
		}
		
		for (int i=0; i<vertices.capacity(); i+=3) {
			IndexedVertex3D indexedVertex3D = new IndexedVertex3D(i / 3, vertices.get(), vertices.get(), vertices.get());
			Set<IndexedVertex3D> set = verticesMap.get(indexedVertex3D);
			if (set == null) {
				set = new HashSet<>();
				verticesMap.put(indexedVertex3D, set);
			}
			set.add(indexedVertex3D);
		}
	}
	
	public void swapVertex(int oldPos, int newPos) {
		for (int i=0; i<3; i++) {
			double tmp = vertices.get(oldPos * 3 + i);
			vertices.put(oldPos * 3 + i, vertices.get(newPos * 3 + i));
			vertices.put(newPos * 3 + i, tmp);
		}
		for (int i=0; i<3; i++) {
			float tmp = normals.get(oldPos * 3 + i);
			normals.put(oldPos * 3 + i, normals.get(newPos * 3 + i));
			normals.put(newPos * 3 + i, tmp);
		}
		Set<Integer> set1 = new HashSet<>(referencedVertices.get(oldPos));
		Set<Integer> set2 = new HashSet<>(referencedVertices.get(newPos));
		for (Integer index : set1) {
			indices.put(index, newPos);
		}
		referencedVertices.put(oldPos, set2);
		referencedVertices.put(newPos, set1);
		
		Set<IndexedVertex3D> set = verticesMap.get(new IndexedVertex3D(0, vertices.get(newPos * 3), vertices.get(newPos * 3 + 1), vertices.get(newPos * 3 + 2)));
		for (IndexedVertex3D indexedVertex3D : set) {
			indexedVertex3D.setOriginalIndex(0);
		}
	}
	
	public IntBuffer getIndices() {
		return indices;
	}
	
	public DoubleBuffer getVertices() {
		return vertices;
	}
	
	public FloatBuffer getNormals() {
		return normals;
	}

	public Mesh copy() {
		IntBuffer newIndices = IntBuffer.allocate(indices.capacity());
		newIndices.put(indices);
		
		DoubleBuffer newVertices = DoubleBuffer.allocate(vertices.capacity());
		newVertices.put(vertices);
		
		FloatBuffer newNormals = FloatBuffer.allocate(normals.capacity());
		newNormals.put(normals);
		
		return new Mesh(newIndices, newVertices, newNormals);
	}
}