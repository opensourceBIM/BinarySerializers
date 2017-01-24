package org.bimserver.serializers.binarygeometry;

/******************************************************************************
 * Copyright (C) 2009-2016  BIMserver.org
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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bimserver.BimserverDatabaseException;
import org.bimserver.emf.PackageMetaData;
import org.bimserver.interfaces.objects.SVector3f;
import org.bimserver.models.geometry.GeometryPackage;
import org.bimserver.plugins.LittleEndianSerializerDataOutputStream;
import org.bimserver.plugins.PluginManagerInterface;
import org.bimserver.plugins.SerializerDataOutputStream;
import org.bimserver.plugins.serializers.MessagingStreamingSerializer;
import org.bimserver.plugins.serializers.ObjectProvider;
import org.bimserver.plugins.serializers.ProgressReporter;
import org.bimserver.plugins.serializers.ProjectInfo;
import org.bimserver.plugins.serializers.SerializerException;
import org.bimserver.shared.HashMapVirtualObject;
import org.bimserver.shared.HashMapWrappedVirtualObject;
import org.eclipse.emf.ecore.EStructuralFeature;

public class BinaryGeometryMessagingStreamingSerializer2 implements MessagingStreamingSerializer {
	
	/*
	 * Format history (starting at version 8):
	 * 
	 * Version 8:
	 * 	- Using short instead of int for indices. SceneJS was converting the indices to Uint16 anyways, so this saves bytes and a conversion on the client-side
	 * Version 9:
	 *  - Sending the materials/colors for splitted geometry as well, before sending the actual parts
	 *  - Aligning bytes to 8s instead of 4s when sending splitted geometry
	 *  - Incrementing splitcounter instead of decrementing (no idea why it was doing that)
	 *  Version 10:
	 *  - Sending the materials/colors for parts as well again
	 */
	
	private static final byte FORMAT_VERSION = 10;
	
	private enum Mode {
		LOAD,
		START,
		DATA,
		END
	}
	
	private enum MessageType {
		INIT((byte)0),
		GEOMETRY_TRIANGLES_PARTED((byte)3),
		GEOMETRY_TRIANGLES((byte)1),
		GEOMETRY_INFO((byte)5),
		END((byte)6);
		
		private byte id;

		private MessageType(byte id) {
			this.id = id;
		}
		
		public byte getId() {
			return id;
		}
	}

	private Mode mode = Mode.LOAD;
	private long splitCounter = 0;
	private ObjectProvider objectProvider;
	private ProjectInfo projectInfo;
	
	@Override
	public void init(ObjectProvider objectProvider, ProjectInfo projectInfo, PluginManagerInterface pluginManager, PackageMetaData packageMetaData) throws SerializerException {
		this.objectProvider = objectProvider;
		this.projectInfo = projectInfo;
	}

	@Override
	public boolean writeMessage(OutputStream outputStream, ProgressReporter progressReporter) throws IOException, SerializerException {
		serializerDataOutputStream = null;
		if (outputStream instanceof SerializerDataOutputStream) {
			serializerDataOutputStream = (SerializerDataOutputStream)outputStream;
		} else {
			serializerDataOutputStream = new LittleEndianSerializerDataOutputStream(outputStream);
		}
		switch (mode) {
		case LOAD: {
			load();
			mode = Mode.START;
		}
		case START:
			writeStart();
			mode = Mode.DATA;
			break;
		case DATA:
			if (!writeData()) {
				mode = Mode.END;
				return true;
			}
			break;
		case END:
			writeEnd();
			return false;
		default:
			break;
		}
		return true;
	}

	private final List<HashMapVirtualObject> infoList = new ArrayList<>();
	private final Map<Long, HashMapVirtualObject> dataList = new HashMap<>();

	private Iterator<HashMapVirtualObject> iterator;
	
	private void load() throws SerializerException {
		HashMapVirtualObject next = null;
		try {
			next = objectProvider.next();
		} catch (BimserverDatabaseException e) {
			throw new SerializerException(e);
		}
		while (next != null) {
			if (next.eClass() == GeometryPackage.eINSTANCE.getGeometryInfo()) {
				infoList.add(next);
			} else if (next.eClass() == GeometryPackage.eINSTANCE.getGeometryData()) {
				dataList.put(next.getOid(), next);
			}
			try {
				next = objectProvider.next();
			} catch (BimserverDatabaseException e) {
				throw new SerializerException(e);
			}
		}
		Collections.sort(infoList, new Comparator<HashMapVirtualObject>(){
			@Override
			public int compare(HashMapVirtualObject o1, HashMapVirtualObject o2) {
				return getBoundingBoxVolume(o2)  - getBoundingBoxVolume(o1);
			}});
		
		iterator = infoList.iterator();
	}
	
	private int getBoundingBoxVolume(HashMapVirtualObject o1) {
		HashMapWrappedVirtualObject minBounds = (HashMapWrappedVirtualObject) o1.eGet(o1.eClass().getEStructuralFeature("minBounds"));
		HashMapWrappedVirtualObject maxBounds = (HashMapWrappedVirtualObject) o1.eGet(o1.eClass().getEStructuralFeature("maxBounds"));
		Double minX = (Double) minBounds.eGet("x");
		Double minY = (Double) minBounds.eGet("y");
		Double minZ = (Double) minBounds.eGet("z");
		Double maxX = (Double) maxBounds.eGet("x");
		Double maxY = (Double) maxBounds.eGet("y");
		Double maxZ = (Double) maxBounds.eGet("z");
		int volume = (int) ((maxX - minX) * (maxY - minY) * (maxZ - minZ));
		return volume;
	}

	private boolean writeEnd() throws IOException {
		serializerDataOutputStream.write(MessageType.END.getId());
		return true;
	}
	
	private void writeStart() throws IOException {
		// Identifier for clients to determine if this server is even serving binary geometry
		serializerDataOutputStream.writeByte(MessageType.INIT.getId());
		serializerDataOutputStream.writeUTF("BGS");
		
		// Version of the current format being outputted, should be changed for every (released) change in protocol 
		serializerDataOutputStream.writeByte(FORMAT_VERSION);
		
		int skip = 4 - (7 % 4);
		if(skip != 0 && skip != 4) {
			serializerDataOutputStream.write(new byte[skip]);
		}

		SVector3f minBounds = projectInfo.getMinBounds();
		serializerDataOutputStream.writeDouble(minBounds.getX());
		serializerDataOutputStream.writeDouble(minBounds.getY());
		serializerDataOutputStream.writeDouble(minBounds.getZ());
		SVector3f maxBounds = projectInfo.getMaxBounds();
		serializerDataOutputStream.writeDouble(maxBounds.getX());
		serializerDataOutputStream.writeDouble(maxBounds.getY());
		serializerDataOutputStream.writeDouble(maxBounds.getZ());
	}
	
	HashMapVirtualObject last = null;

	private SerializerDataOutputStream serializerDataOutputStream;
	
	private boolean writeData() throws IOException, SerializerException {
		HashMapVirtualObject info = null;
		if (last != null) {
			info = last;
			last = null;
		} else {
			info = iterator.next();
		}
		Object transformation = info.eGet(info.eClass().getEStructuralFeature("transformation"));
		Object dataOid = info.eGet(info.eClass().getEStructuralFeature("data"));
		
		HashMapVirtualObject data = dataList.get(dataOid);
		if (data != null) {
			// This geometry info is pointing to a not-yet-sent geometry data, so we send that first
			// This way the client can be sure that geometry data is always available when geometry info is received, simplifying bookkeeping
			last = info;
			EStructuralFeature indicesFeature = data.eClass().getEStructuralFeature("indices");
			EStructuralFeature verticesFeature = data.eClass().getEStructuralFeature("vertices");
			EStructuralFeature normalsFeature = data.eClass().getEStructuralFeature("normals");
			EStructuralFeature materialsFeature = data.eClass().getEStructuralFeature("materials");
			
			byte[] indices = (byte[])data.eGet(indicesFeature);
			byte[] vertices = (byte[])data.eGet(verticesFeature);
			byte[] normals = (byte[])data.eGet(normalsFeature);
			byte[] materials = (byte[])data.eGet(materialsFeature);

			int totalNrIndices = indices.length / 4;
			int maxIndexValues = 16389;

			if (totalNrIndices > maxIndexValues) {
				serializerDataOutputStream.write(MessageType.GEOMETRY_TRIANGLES_PARTED.getId());
				serializerDataOutputStream.write(new byte[7]);
				serializerDataOutputStream.writeLong(data.getOid());
				
				// Split geometry, this algorithm - for now - just throws away all the reuse of vertices that might be there
				// Also, although usually the vertices buffers are too large, this algorithm is based on the indices, so we
				// probably are not cramming as much data as we can in each "part", but that's not really a problem I think

				int nrParts = (totalNrIndices + maxIndexValues - 1) / maxIndexValues;
				serializerDataOutputStream.writeInt(nrParts);

				ByteBuffer indicesBuffer = ByteBuffer.wrap(indices);
				indicesBuffer.order(ByteOrder.LITTLE_ENDIAN);
				IntBuffer indicesIntBuffer = indicesBuffer.asIntBuffer();

				ByteBuffer vertexBuffer = ByteBuffer.wrap(vertices);
				vertexBuffer.order(ByteOrder.LITTLE_ENDIAN);
				FloatBuffer verticesFloatBuffer = vertexBuffer.asFloatBuffer();
				
				ByteBuffer normalsBuffer = ByteBuffer.wrap(normals);
				normalsBuffer.order(ByteOrder.LITTLE_ENDIAN);
				FloatBuffer normalsFloatBuffer = normalsBuffer.asFloatBuffer();

				for (int part=0; part<nrParts; part++) {
					long splitId = splitCounter++;
					serializerDataOutputStream.writeLong(splitId);
					
					short indexCounter = 0;
					int upto = Math.min((part + 1) * maxIndexValues, totalNrIndices);
					serializerDataOutputStream.writeInt(upto - part * maxIndexValues);
					for (int i=part * maxIndexValues; i<upto; i++) {
						serializerDataOutputStream.writeShort(indexCounter++);
					}
					
					// Aligning to 4-bytes
					if ((upto - part * maxIndexValues) % 2 != 0) {
						serializerDataOutputStream.writeShort((short)0);
					}
					
					int nrVertices = (upto - part * maxIndexValues) * 3;
					serializerDataOutputStream.writeInt(nrVertices);
					for (int i=part * maxIndexValues; i<upto; i+=3) {
						int oldIndex1 = indicesIntBuffer.get(i);
						int oldIndex2 = indicesIntBuffer.get(i+1);
						int oldIndex3 = indicesIntBuffer.get(i+2);
						
						serializerDataOutputStream.writeFloat(verticesFloatBuffer.get(oldIndex1 * 3));
						serializerDataOutputStream.writeFloat(verticesFloatBuffer.get(oldIndex1 * 3 + 1));
						serializerDataOutputStream.writeFloat(verticesFloatBuffer.get(oldIndex1 * 3 + 2));
						serializerDataOutputStream.writeFloat(verticesFloatBuffer.get(oldIndex2 * 3));
						serializerDataOutputStream.writeFloat(verticesFloatBuffer.get(oldIndex2 * 3 + 1));
						serializerDataOutputStream.writeFloat(verticesFloatBuffer.get(oldIndex2 * 3 + 2));
						serializerDataOutputStream.writeFloat(verticesFloatBuffer.get(oldIndex3 * 3));
						serializerDataOutputStream.writeFloat(verticesFloatBuffer.get(oldIndex3 * 3 + 1));
						serializerDataOutputStream.writeFloat(verticesFloatBuffer.get(oldIndex3 * 3 + 2));
					}
					serializerDataOutputStream.writeInt(nrVertices);
					for (int i=part * maxIndexValues; i<upto; i+=3) {
						int oldIndex1 = indicesIntBuffer.get(i);
						int oldIndex2 = indicesIntBuffer.get(i+1);
						int oldIndex3 = indicesIntBuffer.get(i+2);
						
						serializerDataOutputStream.writeFloat(normalsFloatBuffer.get(oldIndex1 * 3));
						serializerDataOutputStream.writeFloat(normalsFloatBuffer.get(oldIndex1 * 3 + 1));
						serializerDataOutputStream.writeFloat(normalsFloatBuffer.get(oldIndex1 * 3 + 2));
						serializerDataOutputStream.writeFloat(normalsFloatBuffer.get(oldIndex2 * 3));
						serializerDataOutputStream.writeFloat(normalsFloatBuffer.get(oldIndex2 * 3 + 1));
						serializerDataOutputStream.writeFloat(normalsFloatBuffer.get(oldIndex2 * 3 + 2));
						serializerDataOutputStream.writeFloat(normalsFloatBuffer.get(oldIndex3 * 3));
						serializerDataOutputStream.writeFloat(normalsFloatBuffer.get(oldIndex3 * 3 + 1));
						serializerDataOutputStream.writeFloat(normalsFloatBuffer.get(oldIndex3 * 3 + 2));
					}
					// Only when materials are used we send them
					if (materials != null) {
						ByteBuffer materialsByteBuffer = ByteBuffer.wrap(materials);
						materialsByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
						FloatBuffer materialsFloatBuffer = materialsByteBuffer.asFloatBuffer();

						serializerDataOutputStream.writeInt(nrVertices * 4 / 3);
						for (int i=part * maxIndexValues; i<upto; i+=3) {
							int oldIndex1 = indicesIntBuffer.get(i);
							int oldIndex2 = indicesIntBuffer.get(i+1);
							int oldIndex3 = indicesIntBuffer.get(i+2);

							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex1 * 4));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex1 * 4 + 1));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex1 * 4 + 2));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex1 * 4 + 3));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex2 * 4));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex2 * 4 + 1));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex2 * 4 + 2));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex2 * 4 + 3));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex3 * 4));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex3 * 4 + 1));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex3 * 4 + 2));
							serializerDataOutputStream.writeFloat(materialsFloatBuffer.get(oldIndex3 * 4 + 3));
						}
					} else {
						// No materials used
						serializerDataOutputStream.writeInt(0);
					}
				}
			} else {
				serializerDataOutputStream.write(MessageType.GEOMETRY_TRIANGLES.getId());
				serializerDataOutputStream.write(new byte[7]);
				serializerDataOutputStream.writeLong(data.getOid());
				
				ByteBuffer indicesBuffer = ByteBuffer.wrap(indices);
				indicesBuffer.order(ByteOrder.LITTLE_ENDIAN);
				serializerDataOutputStream.writeInt(indicesBuffer.capacity() / 4);
				IntBuffer intBuffer = indicesBuffer.asIntBuffer();
				for (int i=0; i<intBuffer.capacity(); i++) {
					serializerDataOutputStream.writeShort((short)intBuffer.get());
				}
				
				// Aligning to 4-bytes
				if (intBuffer.capacity() % 2 != 0) {
					serializerDataOutputStream.writeShort((short)0);
				}
				
				ByteBuffer vertexByteBuffer = ByteBuffer.wrap(vertices);
				serializerDataOutputStream.writeInt(vertexByteBuffer.capacity() / 4);
				serializerDataOutputStream.write(vertexByteBuffer.array());
				
				ByteBuffer normalsBuffer = ByteBuffer.wrap(normals);
				serializerDataOutputStream.writeInt(normalsBuffer.capacity() / 4);
				serializerDataOutputStream.write(normalsBuffer.array());
				
				// Only when materials are used we send them
				if (materials != null) {
					ByteBuffer materialsByteBuffer = ByteBuffer.wrap(materials);
					
					serializerDataOutputStream.writeInt(materialsByteBuffer.capacity() / 4);
					serializerDataOutputStream.write(materialsByteBuffer.array());
				} else {
					// No materials used
					serializerDataOutputStream.writeInt(0);
				}
			}
			dataList.remove(dataOid);
			return true;
		}
		
		serializerDataOutputStream.writeByte(MessageType.GEOMETRY_INFO.getId());
		serializerDataOutputStream.write(new byte[7]);
		serializerDataOutputStream.writeLong(info.getRoid());
		serializerDataOutputStream.writeLong(info.getOid());
		HashMapWrappedVirtualObject minBounds = (HashMapWrappedVirtualObject) info.eGet(info.eClass().getEStructuralFeature("minBounds"));
		HashMapWrappedVirtualObject maxBounds = (HashMapWrappedVirtualObject) info.eGet(info.eClass().getEStructuralFeature("maxBounds"));
		Double minX = (Double) minBounds.eGet("x");
		Double minY = (Double) minBounds.eGet("y");
		Double minZ = (Double) minBounds.eGet("z");
		Double maxX = (Double) maxBounds.eGet("x");
		Double maxY = (Double) maxBounds.eGet("y");
		Double maxZ = (Double) maxBounds.eGet("z");
		serializerDataOutputStream.writeDouble(minX);
		serializerDataOutputStream.writeDouble(minY);
		serializerDataOutputStream.writeDouble(minZ);
		serializerDataOutputStream.writeDouble(maxX);
		serializerDataOutputStream.writeDouble(maxY);
		serializerDataOutputStream.writeDouble(maxZ);
		serializerDataOutputStream.write((byte[])transformation);
		serializerDataOutputStream.writeLong((Long)dataOid);
		
		return iterator.hasNext();
	}
}