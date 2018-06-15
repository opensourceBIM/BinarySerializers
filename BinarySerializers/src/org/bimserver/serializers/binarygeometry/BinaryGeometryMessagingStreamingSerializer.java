package org.bimserver.serializers.binarygeometry;

/******************************************************************************
 * Copyright (C) 2009-2018  BIMserver.org
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

import org.bimserver.BimserverDatabaseException;
import org.bimserver.database.queries.om.QueryException;
import org.bimserver.emf.PackageMetaData;
import org.bimserver.geometry.Matrix;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class BinaryGeometryMessagingStreamingSerializer implements MessagingStreamingSerializer {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinaryGeometryMessagingStreamingSerializer.class);

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
	 *  Version 11:
	 *  - Added ability to send one specific color for all geometry contained in a GeometryData object
	 *  Version 12:
	 *  - Added boolean value that indicates whether an object/geometry has transparency
	 *  Version 13:
	 *  - Added integer value that indicates how many times geometry is being reused
	 *  Version 14:
	 *  - Added ifcproduct oid to simplify client-side operations, also added type of object, also added type for GeometryData
	 */
	
	private static final byte FORMAT_VERSION = 14;
	
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

	private boolean splitGeometry = false;
	private boolean useSingleColors = false;
	private boolean quantitizeNormals = false;
	private boolean quantitizeVertices = false;
	private float[] vertexQuantizationMatrix;
	
	private Mode mode = Mode.LOAD;
	private long splitCounter = 0;
	private ObjectProvider objectProvider;
	private ProjectInfo projectInfo;
	private SerializerDataOutputStream serializerDataOutputStream;
	private HashMapVirtualObject next;
	private ProgressReporter progressReporter;
	private int nrObjectsWritten;
	private int size;

	private Bounds modelBounds;

	private Bounds modelBoundsUntranslated;

	@Override
	public void init(ObjectProvider objectProvider, ProjectInfo projectInfo, PluginManagerInterface pluginManager, PackageMetaData packageMetaData) throws SerializerException {
		this.objectProvider = objectProvider;
		this.projectInfo = projectInfo;
		ObjectNode queryNode = objectProvider.getQueryNode();
		if (queryNode.has("geometrySettings")) {
			ObjectNode geometrySettings = (ObjectNode) queryNode.get("geometrySettings");
			
			System.out.println(geometrySettings.toString());
			
			useSingleColors = geometrySettings.has("useSingleColorPerObject") && geometrySettings.get("useSingleColorPerObject").asBoolean();
			splitGeometry = geometrySettings.has("splitGeometry") && geometrySettings.get("splitGeometry").asBoolean();
			quantitizeNormals = geometrySettings.has("quantitizeNormals") && geometrySettings.get("quantitizeNormals").asBoolean();
			quantitizeVertices = geometrySettings.has("quantitizeVertices") && geometrySettings.get("quantitizeVertices").asBoolean();
			if (quantitizeVertices) {
				vertexQuantizationMatrix = new float[16];
				ArrayNode vqmNode = (ArrayNode) geometrySettings.get("vertexQuantizationMatrix");
				int i=0;
				for (JsonNode v : vqmNode) {
					vertexQuantizationMatrix[i++] = v.floatValue();
				}
			}
		}
	}

	@Override
	public boolean writeMessage(OutputStream outputStream, ProgressReporter progressReporter) throws IOException, SerializerException {
		this.progressReporter = progressReporter;
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
			// Explicitly no break here, move on to start right away
		}
		case START:
			writeStart();
			if (next == null) {
				mode = Mode.END;
			} else {
				mode = Mode.DATA;
			}
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

	private void load() throws SerializerException {
//		long start = System.nanoTime();
		size = 0;
		HashMapVirtualObject next = null;
		try {
			next = objectProvider.next();
			while (next != null) {
				if (next.eClass() == GeometryPackage.eINSTANCE.getGeometryInfo()) {
					size++;
				}
				next = objectProvider.next();
			}
		} catch (BimserverDatabaseException e) {
			throw new SerializerException(e);
		}
		try {
			objectProvider = objectProvider.copy();
			this.next = objectProvider.next();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (QueryException e) {
			e.printStackTrace();
		} catch (BimserverDatabaseException e) {
			e.printStackTrace();
		}
//		long end = System.nanoTime();
//		System.out.println(((end - start) / 1000000) + " ms prepare time");
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
		
		serializerDataOutputStream.align8();

		SVector3f minBounds = projectInfo.getMinBounds();
		serializerDataOutputStream.writeDouble(minBounds.getX());
		serializerDataOutputStream.writeDouble(minBounds.getY());
		serializerDataOutputStream.writeDouble(minBounds.getZ());
		SVector3f maxBounds = projectInfo.getMaxBounds();
		serializerDataOutputStream.writeDouble(maxBounds.getX());
		serializerDataOutputStream.writeDouble(maxBounds.getY());
		serializerDataOutputStream.writeDouble(maxBounds.getZ());
		
		modelBounds = new Bounds(minBounds, maxBounds);
		modelBoundsUntranslated = new Bounds(projectInfo.getBoundsUntranslated());
	}

	private boolean writeData() throws IOException, SerializerException {
		if (next == null) {
			return false;
		}
		if (GeometryPackage.eINSTANCE.getGeometryInfo() == next.eClass()) {
			HashMapVirtualObject info = next;
			EStructuralFeature hasTransparencyFeature = info.eClass().getEStructuralFeature("hasTransparency");
			Object transformation = info.eGet(info.eClass().getEStructuralFeature("transformation"));
			Object dataOid = info.eGet(info.eClass().getEStructuralFeature("data"));
			
			serializerDataOutputStream.writeByte(MessageType.GEOMETRY_INFO.getId());
			long oid = (long) next.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_IfcProductOid());
			serializerDataOutputStream.writeLong(oid);
			String type = objectProvider.getEClassForOid(oid).getName();
			serializerDataOutputStream.writeUTF(type);
			serializerDataOutputStream.align8();
			serializerDataOutputStream.writeLong(info.getRoid());
			serializerDataOutputStream.writeLong(info.getOid());
			serializerDataOutputStream.writeLong((boolean)info.eGet(hasTransparencyFeature) ? 1 : 0);
			HashMapWrappedVirtualObject bounds = (HashMapWrappedVirtualObject) info.eGet(info.eClass().getEStructuralFeature("bounds"));
			HashMapWrappedVirtualObject minBounds = (HashMapWrappedVirtualObject) bounds.eGet(bounds.eClass().getEStructuralFeature("min"));
			HashMapWrappedVirtualObject maxBounds = (HashMapWrappedVirtualObject) bounds.eGet(bounds.eClass().getEStructuralFeature("max"));
			Double minX = (Double) minBounds.eGet("x");
			Double minY = (Double) minBounds.eGet("y");
			Double minZ = (Double) minBounds.eGet("z");
			Double maxX = (Double) maxBounds.eGet("x");
			Double maxY = (Double) maxBounds.eGet("y");
			Double maxZ = (Double) maxBounds.eGet("z");
			
			serializerDataOutputStream.ensureExtraCapacity(8 * 6 + ((byte[])transformation).length + 8);
			serializerDataOutputStream.writeDoubleUnchecked(minX);
			serializerDataOutputStream.writeDoubleUnchecked(minY);
			serializerDataOutputStream.writeDoubleUnchecked(minZ);
			serializerDataOutputStream.writeDoubleUnchecked(maxX);
			serializerDataOutputStream.writeDoubleUnchecked(maxY);
			serializerDataOutputStream.writeDoubleUnchecked(maxZ);
			serializerDataOutputStream.write((byte[])transformation);
			serializerDataOutputStream.writeLongUnchecked((Long)dataOid);
			
			nrObjectsWritten++;
			if (progressReporter != null) {
				progressReporter.update(nrObjectsWritten, size);
			}
		} else if (GeometryPackage.eINSTANCE.getGeometryData() == next.eClass()) {
			HashMapVirtualObject data = next;
			// This geometry info is pointing to a not-yet-sent geometry data, so we send that first
			// This way the client can be sure that geometry data is always available when geometry info is received, simplifying bookkeeping
			EStructuralFeature indicesFeature = data.eClass().getEStructuralFeature("indices");
			EStructuralFeature verticesFeature = data.eClass().getEStructuralFeature("vertices");
			EStructuralFeature normalsFeature = data.eClass().getEStructuralFeature("normals");
			EStructuralFeature materialsFeature = data.eClass().getEStructuralFeature("materials");
			EStructuralFeature colorFeature = data.eClass().getEStructuralFeature("color");
			EStructuralFeature mostUsedColorFeature = data.eClass().getEStructuralFeature("mostUsedColor");
			EStructuralFeature hasTransparencyFeature = data.eClass().getEStructuralFeature("hasTransparency");
			
			byte[] indices = (byte[])data.eGet(indicesFeature);
			byte[] vertices = (byte[])data.eGet(verticesFeature);
			byte[] normals = (byte[])data.eGet(normalsFeature);
			byte[] materials = (byte[])data.eGet(materialsFeature);
			HashMapWrappedVirtualObject color = (HashMapWrappedVirtualObject)data.eGet(colorFeature);
			HashMapWrappedVirtualObject mostUsedColor = (HashMapWrappedVirtualObject)data.eGet(mostUsedColorFeature);

			int totalNrIndices = indices.length / 4;
			int maxIndexValues = 16389;

			if (splitGeometry) {
				if (totalNrIndices > maxIndexValues) {
					serializerDataOutputStream.writeByte(MessageType.GEOMETRY_TRIANGLES_PARTED.getId());
					serializerDataOutputStream.writeInt((int) data.get("reused"));
					serializerDataOutputStream.align8();
					serializerDataOutputStream.writeLong((boolean)data.eGet(hasTransparencyFeature) ? 1 : 0);
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
						serializerDataOutputStream.ensureExtraCapacity(2 * (upto - part * maxIndexValues));
						for (int i=part * maxIndexValues; i<upto; i++) {
							serializerDataOutputStream.writeShortUnchecked(indexCounter++);
						}
						
						// Added in version 11
						if (color != null) {
							serializerDataOutputStream.writeInt(1);
							serializerDataOutputStream.writeFloat((float)color.eGet("x"));
							serializerDataOutputStream.writeFloat((float)color.eGet("y"));
							serializerDataOutputStream.writeFloat((float)color.eGet("z"));
							serializerDataOutputStream.writeFloat((float)color.eGet("w"));
						} else {
							serializerDataOutputStream.writeInt(0);
						}
						
						serializerDataOutputStream.align4();
						
						int nrVertices = (upto - part * maxIndexValues) * 3;
						serializerDataOutputStream.writeInt(nrVertices);
						serializerDataOutputStream.ensureExtraCapacity(12 * (upto - part * maxIndexValues));
						
						for (int i=part * maxIndexValues; i<upto; i+=3) {
							int oldIndex1 = indicesIntBuffer.get(i);
							int oldIndex2 = indicesIntBuffer.get(i+1);
							int oldIndex3 = indicesIntBuffer.get(i+2);
							
							serializerDataOutputStream.writeFloatUnchecked(verticesFloatBuffer.get(oldIndex1 * 3));
							serializerDataOutputStream.writeFloatUnchecked(verticesFloatBuffer.get(oldIndex1 * 3 + 1));
							serializerDataOutputStream.writeFloatUnchecked(verticesFloatBuffer.get(oldIndex1 * 3 + 2));
							serializerDataOutputStream.writeFloatUnchecked(verticesFloatBuffer.get(oldIndex2 * 3));
							serializerDataOutputStream.writeFloatUnchecked(verticesFloatBuffer.get(oldIndex2 * 3 + 1));
							serializerDataOutputStream.writeFloatUnchecked(verticesFloatBuffer.get(oldIndex2 * 3 + 2));
							serializerDataOutputStream.writeFloatUnchecked(verticesFloatBuffer.get(oldIndex3 * 3));
							serializerDataOutputStream.writeFloatUnchecked(verticesFloatBuffer.get(oldIndex3 * 3 + 1));
							serializerDataOutputStream.writeFloatUnchecked(verticesFloatBuffer.get(oldIndex3 * 3 + 2));
						}
						serializerDataOutputStream.writeInt(nrVertices);
						serializerDataOutputStream.ensureExtraCapacity(12 * (upto - part * maxIndexValues));
						for (int i=part * maxIndexValues; i<upto; i+=3) {
							int oldIndex1 = indicesIntBuffer.get(i);
							int oldIndex2 = indicesIntBuffer.get(i+1);
							int oldIndex3 = indicesIntBuffer.get(i+2);
							
							serializerDataOutputStream.writeFloatUnchecked(normalsFloatBuffer.get(oldIndex1 * 3));
							serializerDataOutputStream.writeFloatUnchecked(normalsFloatBuffer.get(oldIndex1 * 3 + 1));
							serializerDataOutputStream.writeFloatUnchecked(normalsFloatBuffer.get(oldIndex1 * 3 + 2));
							serializerDataOutputStream.writeFloatUnchecked(normalsFloatBuffer.get(oldIndex2 * 3));
							serializerDataOutputStream.writeFloatUnchecked(normalsFloatBuffer.get(oldIndex2 * 3 + 1));
							serializerDataOutputStream.writeFloatUnchecked(normalsFloatBuffer.get(oldIndex2 * 3 + 2));
							serializerDataOutputStream.writeFloatUnchecked(normalsFloatBuffer.get(oldIndex3 * 3));
							serializerDataOutputStream.writeFloatUnchecked(normalsFloatBuffer.get(oldIndex3 * 3 + 1));
							serializerDataOutputStream.writeFloatUnchecked(normalsFloatBuffer.get(oldIndex3 * 3 + 2));
						}
						// Only when materials are used we send them
						if (materials != null && color == null) {
							ByteBuffer materialsByteBuffer = ByteBuffer.wrap(materials);
							materialsByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
							FloatBuffer materialsFloatBuffer = materialsByteBuffer.asFloatBuffer();

							serializerDataOutputStream.writeInt(nrVertices * 4 / 3);
							serializerDataOutputStream.ensureExtraCapacity(16 * (upto - part * maxIndexValues));
							for (int i=part * maxIndexValues; i<upto; i+=3) {
								int oldIndex1 = indicesIntBuffer.get(i);
								int oldIndex2 = indicesIntBuffer.get(i+1);
								int oldIndex3 = indicesIntBuffer.get(i+2);

								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex1 * 4));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex1 * 4 + 1));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex1 * 4 + 2));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex1 * 4 + 3));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex2 * 4));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex2 * 4 + 1));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex2 * 4 + 2));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex2 * 4 + 3));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex3 * 4));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex3 * 4 + 1));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex3 * 4 + 2));
								serializerDataOutputStream.writeFloatUnchecked(materialsFloatBuffer.get(oldIndex3 * 4 + 3));
							}
						} else {
							// No materials used
							serializerDataOutputStream.writeInt(0);
						}
					}
				} else {
					serializerDataOutputStream.writeByte(MessageType.GEOMETRY_TRIANGLES.getId());
					serializerDataOutputStream.writeInt((int) data.get("reused"));
					
					short cid = (short) data.get("type");
					String type = objectProvider.getEClassForCid(cid).getName();
					serializerDataOutputStream.writeUTF(type);
					serializerDataOutputStream.align8();
					
					serializerDataOutputStream.writeLong((boolean)data.eGet(hasTransparencyFeature) ? 1 : 0);
					serializerDataOutputStream.writeLong(data.getOid());
					
					ByteBuffer indicesBuffer = ByteBuffer.wrap(indices);
					indicesBuffer.order(ByteOrder.LITTLE_ENDIAN);
					serializerDataOutputStream.writeInt(indicesBuffer.capacity() / 4);
					IntBuffer intBuffer = indicesBuffer.asIntBuffer();
					serializerDataOutputStream.ensureExtraCapacity(intBuffer.capacity());
					for (int i=0; i<intBuffer.capacity(); i++) {
						serializerDataOutputStream.writeShortUnchecked((short)intBuffer.get());
					}
					
					// Added in version 11
					if (color != null) {
						serializerDataOutputStream.writeInt(1);
						serializerDataOutputStream.writeFloat((float)color.eGet("x"));
						serializerDataOutputStream.writeFloat((float)color.eGet("y"));
						serializerDataOutputStream.writeFloat((float)color.eGet("z"));
						serializerDataOutputStream.writeFloat((float)color.eGet("w"));
					} else {
						serializerDataOutputStream.writeInt(0);
					}
					
					serializerDataOutputStream.align4();

					ByteBuffer vertexByteBuffer = ByteBuffer.wrap(vertices);
					serializerDataOutputStream.writeInt(vertexByteBuffer.capacity() / 4);
					serializerDataOutputStream.write(vertexByteBuffer.array());
					
					ByteBuffer normalsBuffer = ByteBuffer.wrap(normals);
					serializerDataOutputStream.writeInt(normalsBuffer.capacity() / 4);
					serializerDataOutputStream.write(normalsBuffer.array());
					
					// Only when materials are used we send them
					if (materials != null && color == null) {
						ByteBuffer materialsByteBuffer = ByteBuffer.wrap(materials);
						
						serializerDataOutputStream.writeInt(materialsByteBuffer.capacity() / 4);
						serializerDataOutputStream.write(materialsByteBuffer.array());
					} else {
						// No materials used
						serializerDataOutputStream.writeInt(0);
					}
				}
			} else {
				serializerDataOutputStream.writeByte(MessageType.GEOMETRY_TRIANGLES.getId());
				serializerDataOutputStream.writeInt((int) data.get("reused"));

				short cid = (short) data.get("type");
				String type = objectProvider.getEClassForCid(cid).getName();
				serializerDataOutputStream.writeUTF(type);
				serializerDataOutputStream.align8();

				serializerDataOutputStream.writeLong((boolean)data.eGet(hasTransparencyFeature) ? 1 : 0);
				serializerDataOutputStream.writeLong(data.getOid());
				
				ByteBuffer indicesBuffer = ByteBuffer.wrap(indices);
				indicesBuffer.order(ByteOrder.LITTLE_ENDIAN);
				serializerDataOutputStream.writeInt(indicesBuffer.capacity() / 4);
				IntBuffer intBuffer = indicesBuffer.asIntBuffer();
				for (int i=0; i<intBuffer.capacity(); i++) {
					serializerDataOutputStream.writeIntUnchecked(intBuffer.get());
				}

				// Added in version 11
				if (color != null) {
					serializerDataOutputStream.writeInt(1);
					serializerDataOutputStream.writeFloat((float)color.eGet("x"));
					serializerDataOutputStream.writeFloat((float)color.eGet("y"));
					serializerDataOutputStream.writeFloat((float)color.eGet("z"));
					serializerDataOutputStream.writeFloat((float)color.eGet("w"));
				} else {
					if (useSingleColors && mostUsedColor != null) {
						serializerDataOutputStream.writeInt(1);
						serializerDataOutputStream.writeFloat((float)mostUsedColor.eGet("x"));
						serializerDataOutputStream.writeFloat((float)mostUsedColor.eGet("y"));
						serializerDataOutputStream.writeFloat((float)mostUsedColor.eGet("z"));
						serializerDataOutputStream.writeFloat((float)mostUsedColor.eGet("w"));
					} else {
						serializerDataOutputStream.writeInt(0);
					}
				}

				ByteBuffer vertexByteBuffer = ByteBuffer.wrap(vertices);
				serializerDataOutputStream.writeInt(vertexByteBuffer.capacity() / 4);
				if (quantitizeVertices) {
					vertexByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
					serializerDataOutputStream.ensureExtraCapacity(vertexByteBuffer.capacity() * 6 / 4);
					for (int i=0; i<vertexByteBuffer.capacity() / 4; i+=3) {
						float[] vertex = new float[]{vertexByteBuffer.getFloat(), vertexByteBuffer.getFloat(), vertexByteBuffer.getFloat(), 1};
						
						if (!modelBoundsUntranslated.in(vertex)) {
							LOGGER.error("Untranslated vertex outside of Untranslated model bounds, something must have gone wrong in generation");
							LOGGER.error(vertex[0] + ", " + vertex[1] + ", " + vertex[2]);
							LOGGER.error(modelBoundsUntranslated.toString());
						}
						
						float[] result = new float[4];
						Matrix.multiplyMV(result, 0, vertexQuantizationMatrix, 0, vertex, 0);
						
						for (float f : result) {
							if (f > Short.MAX_VALUE || f < Short.MIN_VALUE) {
								LOGGER.error("Result of multiplication too large: " + f);
							}
						}
						
						serializerDataOutputStream.writeShortUnchecked((short)result[0]);
						serializerDataOutputStream.writeShortUnchecked((short)result[1]);
						serializerDataOutputStream.writeShortUnchecked((short)result[2]);
					}
					serializerDataOutputStream.align8();
				} else {
					serializerDataOutputStream.write(vertexByteBuffer.array());
				}
				
				ByteBuffer normalsBuffer = ByteBuffer.wrap(normals);
				serializerDataOutputStream.writeInt(normalsBuffer.capacity() / 4);
				if (quantitizeNormals) {
					normalsBuffer.order(ByteOrder.LITTLE_ENDIAN);
					for (int i=0; i<normalsBuffer.capacity() / 4; i++) {
						float normal = normalsBuffer.getFloat();
						serializerDataOutputStream.writeByteUnchecked((int) (normal * 127));
					}
					serializerDataOutputStream.align8();
				} else {
					serializerDataOutputStream.write(normalsBuffer.array());
				}
				
				// Only when materials are used we send them
				if (!useSingleColors && materials != null && color == null) {
					ByteBuffer materialsByteBuffer = ByteBuffer.wrap(materials);
					
					serializerDataOutputStream.writeInt(materialsByteBuffer.capacity() / 4);
					serializerDataOutputStream.write(materialsByteBuffer.array());
				} else {
					// No materials used
					serializerDataOutputStream.writeInt(0);
				}
			}
		}
		try {
			next = objectProvider.next();
		} catch (BimserverDatabaseException e) {
			e.printStackTrace();
		}
		return next != null;
	}

	@Override
	public void close() {
	}
}