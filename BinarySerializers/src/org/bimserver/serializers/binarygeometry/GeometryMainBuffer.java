package org.bimserver.serializers.binarygeometry;

import java.util.ArrayList;
import java.util.List;

public class GeometryMainBuffer {

	private final int MAX_SIZE = 100 * 1024 * 1024; // 100MB
	
	private final List<GeometryBuffer> buffers = new ArrayList<>();
	private GeometryBuffer currentBuffer;
	
	private int currentIndex = 0;
	
	public GeometryMainBuffer() {
	}
	
	public GeometryBuffer getCurrentWriteBuffer() {
		if (currentBuffer == null || currentBuffer.getPreparedByteSize() > MAX_SIZE) {
			currentBuffer = new GeometryBuffer();
			buffers.add(currentBuffer);
		}
		return currentBuffer;
	}

	public boolean isEmpty() {
		return buffers.isEmpty();
	}

	public boolean hasNextReadBuffer() {
		return currentIndex < buffers.size() - 1;
	}

	public GeometryBuffer getCurrentReadBuffer() {
		if (buffers.size() == 0) {
			return null;
		}
		return buffers.get(currentIndex);
	}
	
	public GeometryBuffer getNextReadBuffer() {
		currentIndex++;
		GeometryBuffer geometryBuffer = buffers.get(currentIndex);
		return geometryBuffer;
	}
}
