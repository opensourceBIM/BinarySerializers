package org.bimserver.serializers.binarygeometry;

public class MeshChanger {
	private Mesh original;

	public MeshChanger(Mesh original, Mesh newMesh) {
		this.original = original;
	}
	
	public Mesh getOriginal() {
		return original;
	}
}
