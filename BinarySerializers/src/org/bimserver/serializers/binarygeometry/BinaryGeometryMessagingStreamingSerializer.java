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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.bimserver.BimserverDatabaseException;
import org.bimserver.database.queries.om.QueryException;
import org.bimserver.emf.PackageMetaData;
import org.bimserver.geometry.Matrix;
import org.bimserver.geometry.Matrix3;
import org.bimserver.geometry.Vector;
import org.bimserver.geometry.Vector3D;
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
import org.bimserver.serializers.binarygeometry.clipping.Point;
import org.bimserver.shared.AbstractHashMapVirtualObject;
import org.bimserver.shared.HashMapVirtualObject;
import org.bimserver.shared.HashMapWrappedVirtualObject;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.primitives.UnsignedBytes;

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
	 *  Version 15:
	 *  - Also sending a multiplier to convert to mm
	 *  Version 16:
	 *  - Just a version bump to make sure older client will err-out
	 *  Version 17:
	 *  - Sending the amount of colors now for GeometryInfo and also sending 1 byte to indicate whether geometry is in a completeBuffer
	 *  - Added writePreparedBuffer (should not have an impact if you don't send the prepareBuffers option)
	 *  Version 18:
	 *  - Oct-encoding of normals, mat4 globalTransformation input is now vec3 globalTranslationVector
	 *  Version 19:
	 *  - Added optional generateLineRenders, protocol has changed because the counts are always sent regardless of setting
	 *  Version 20:
	 *  - Added ifcProductPid
	 */
	
	private static final byte FORMAT_VERSION = 20;
	
	private enum Mode {
		LOAD,
		START,
		DATA,
		PREPARED_BUFFER_INIT,
		PREPARED_BUFFER_TRANSPARENT,
		PREPARED_BUFFER_OPAQUE,
		END
	}
	
	private enum MessageType {
		INIT((byte)0),
		GEOMETRY_TRIANGLES_PARTED((byte)3),
		GEOMETRY_TRIANGLES((byte)1),
		GEOMETRY_INFO((byte)5),
		MINIMAL_GEOMETRY_INFO((byte)9),
		PREPARED_BUFFER_TRANSPARENT((byte)7),
		PREPARED_BUFFER_OPAQUE((byte)8),
		PREPARED_BUFFER_TRANSPARENT_INIT((byte)10),
		PREPARED_BUFFER_OPAQUE_INIT((byte)11),
		END((byte)6);
		
		private byte id;

		private MessageType(byte id) {
			this.id = id;
		}
		
		public byte getId() {
			return id;
		}
	}

	private boolean splitGeometry = true;
	private boolean useSingleColors = false;
	private boolean quantizeNormals = false;
	private boolean octEncodeNormals = false;
	private boolean quantizeVertices = false;
	private boolean quantizeColors = false;
	private boolean prepareBuffers = false;
	private boolean normalizeUnitsToMM = false;
	private boolean useSmallInts = true;
	private boolean reportProgress = true;
	private boolean useUuidAndRid = false;
	private Map<Long, float[]> vertexQuantizationMatrices;
	
	private GeometryMainBuffer transparentGeometryBuffer;
	private GeometryMainBuffer opaqueGeometryBuffer;
	
	private final Map<Long, HashMapVirtualObject> oidToGeometryData = new HashMap<>();
	private final Map<Long, HashMapVirtualObject> dataToGeometryInfo = new HashMap<>();
	private double[] vertexQuantizationMatrix;
	
	private Mode mode = Mode.LOAD;
	private long splitCounter = 0;
	private ObjectProvider objectProvider;
	private ProjectInfo projectInfo;
	private SerializerDataOutputStream serializerDataOutputStream;
	private HashMapVirtualObject next;
	private ProgressReporter progressReporter;
	private int nrObjectsWritten;
	private int size;

	private ByteBuffer lastTransformation;

	private Set<Long> reusedDataOids;

	private double[] globalTranslationVector;

	private boolean generateLineRenders;

	private float[] lastNormal;

	@Override
	public void init(ObjectProvider objectProvider, ProjectInfo projectInfo, PluginManagerInterface pluginManager, PackageMetaData packageMetaData) throws SerializerException {
		this.objectProvider = objectProvider;
		this.projectInfo = projectInfo;
		ObjectNode queryNode = objectProvider.getQueryNode();
		if (queryNode.has("tiles")) {
			ObjectNode tilesNode = (ObjectNode)queryNode.get("tiles");
			if (tilesNode.has("geometryDataToReuse") && !tilesNode.get("geometryDataToReuse").isNull()) {
				ArrayNode reuseNodes = (ArrayNode)tilesNode.get("geometryDataToReuse");
				this.reusedDataOids = new HashSet<>();
				for (JsonNode jsonNode : reuseNodes) {
					this.reusedDataOids.add(jsonNode.asLong());
				}
			}
		}
		if (queryNode.has("loaderSettings")) {
			ObjectNode geometrySettings = (ObjectNode) queryNode.get("loaderSettings");
			
			useUuidAndRid = geometrySettings.has("useUuidAndRid") && geometrySettings.get("useUuidAndRid").asBoolean();
			useSingleColors = geometrySettings.has("useObjectColors") && geometrySettings.get("useObjectColors").asBoolean();
			splitGeometry = geometrySettings.has("splitGeometry") && geometrySettings.get("splitGeometry").asBoolean();
			quantizeNormals = geometrySettings.has("quantizeNormals") && geometrySettings.get("quantizeNormals").asBoolean();
			octEncodeNormals = geometrySettings.has("octEncodeNormals") && geometrySettings.get("octEncodeNormals").asBoolean();
			quantizeVertices = geometrySettings.has("quantizeVertices") && geometrySettings.get("quantizeVertices").asBoolean();
			quantizeColors = geometrySettings.has("quantizeColors") && geometrySettings.get("quantizeColors").asBoolean();
			normalizeUnitsToMM = geometrySettings.has("normalizeUnitsToMM") && geometrySettings.get("normalizeUnitsToMM").asBoolean();
			reportProgress = !geometrySettings.has("reportProgress") || geometrySettings.get("reportProgress").asBoolean(); // default is true for backwards compat
			useSmallInts = !geometrySettings.has("useSmallInts") || geometrySettings.get("useSmallInts").asBoolean(); // default is true for backwards compat
			prepareBuffers = geometrySettings.has("prepareBuffers") && geometrySettings.get("prepareBuffers").asBoolean();
			generateLineRenders = geometrySettings.has("generateLineRenders") && geometrySettings.get("generateLineRenders").asBoolean();
			if (geometrySettings.has("globalTranslationVector")) {
				this.globalTranslationVector = new double[3];
				ArrayNode matrixNode = (ArrayNode) geometrySettings.get("globalTranslationVector");
				int i=0;
				for (JsonNode v : matrixNode) {
					this.globalTranslationVector[i++] = v.asDouble();
				}
			}
			if (prepareBuffers) {
				transparentGeometryBuffer = new GeometryMainBuffer();
				opaqueGeometryBuffer = new GeometryMainBuffer();
			}
			if (quantizeVertices) {
				if (queryNode.has("loaderSettings")) {
					ArrayNode vqmNode = (ArrayNode) geometrySettings.get("vertexQuantizationMatrix");
					if (vqmNode != null) {
						vertexQuantizationMatrix = new double[16];
						int i=0;
						for (JsonNode v : vqmNode) {
							vertexQuantizationMatrix[i++] = v.doubleValue();
						}
//						Matrix.dump(vertexQuantizationMatrix);
					}
				}

				ObjectNode vqmNode = (ObjectNode) geometrySettings.get("vertexQuantizationMatrices");
				if (vqmNode != null) {
					Iterator<String> fieldNames = vqmNode.fieldNames();
					vertexQuantizationMatrices = new HashMap<>();
					while (fieldNames.hasNext()) {
						String key = fieldNames.next();
						long croid = Long.parseLong(key);
						float[] vertexQuantizationMatrix = new float[16];
						ArrayNode mNode = (ArrayNode) vqmNode.get(key);
						vertexQuantizationMatrices.put(croid, vertexQuantizationMatrix);
						int i=0;
						for (JsonNode v : mNode) {
							vertexQuantizationMatrix[i++] = v.floatValue();
						}
					}
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
			serializerDataOutputStream.align8();
			if (next == null) {
				mode = Mode.END;
			} else {
				mode = Mode.DATA;
			}
			break;
		case DATA:
			if (!writeData()) {
				serializerDataOutputStream.align8();
				if (prepareBuffers) {
					mode = Mode.PREPARED_BUFFER_OPAQUE;
				} else {
					mode = Mode.END;
				}
				return true;
			}
			serializerDataOutputStream.align8();
			break;
		case PREPARED_BUFFER_OPAQUE:
			if (!writePreparedBuffer(mode)) {
				mode = Mode.PREPARED_BUFFER_TRANSPARENT;
			}
			break;
		case PREPARED_BUFFER_TRANSPARENT:
			if (!writePreparedBuffer(mode)) {
				mode = Mode.END;
			}
			break;
		case END:
			writeEnd();
			serializerDataOutputStream.align8();
			return false;
		default:
			break;
		}
		return true;
	}

	private boolean writePreparedBuffer(Mode mode) throws IOException, SerializerException {
		try {
			GeometryMainBuffer geometryMainBuffer = getCurrentMainBuffer();
			if (geometryMainBuffer == null) {
				return false;
			}
			GeometryBuffer geometryBuffer = geometryMainBuffer.getCurrentReadBuffer();
			while (geometryBuffer == null || geometryBuffer.isEmpty() || !geometryBuffer.hasNextGeometryMapping()) {
				if (geometryMainBuffer.hasNextReadBuffer()) {
					geometryBuffer = geometryMainBuffer.getNextReadBuffer();
				} else {
					return false;
				}
			}
			if (!geometryBuffer.initSent()) {
				ByteBuffer buffer = ByteBuffer.allocate(25).order(ByteOrder.LITTLE_ENDIAN);

				buffer.put(mode == Mode.PREPARED_BUFFER_OPAQUE ? MessageType.PREPARED_BUFFER_OPAQUE_INIT.getId() : MessageType.PREPARED_BUFFER_TRANSPARENT_INIT.getId());
				buffer.putInt(geometryBuffer.getNrObjects());
				buffer.putInt(geometryBuffer.getNrIndices());
				buffer.putInt(generateLineRenders ? geometryBuffer.getNrLineIndices() : 0);
				buffer.putInt(geometryBuffer.getNrVertices());
				buffer.putInt(geometryBuffer.getNrVertices());
				buffer.putInt(geometryBuffer.getNrColors());
	
				serializerDataOutputStream.write(buffer.array());
				serializerDataOutputStream.align8();
				
				geometryBuffer.setInitSent();
				
				return true;
			}
			int vertexPosition = 0;
			serializerDataOutputStream.writeByte(mode == Mode.PREPARED_BUFFER_TRANSPARENT ? 7 : 8);
			GeometrySubBuffer geometryMapping = geometryBuffer.getNextGeometryMapping();
			ByteBuffer buffer = ByteBuffer.allocate(geometryMapping.getPreparedByteSize()).order(ByteOrder.LITTLE_ENDIAN);
	
			buffer.putInt(geometryMapping.getNrObjects());
			buffer.putInt(geometryMapping.getNrIndices());
			buffer.putInt(generateLineRenders ? geometryMapping.getNrLineIndices() : 0);
			buffer.putInt(geometryMapping.getNrVertices());
			buffer.putInt(geometryMapping.getNrVertices());
			buffer.putInt(geometryMapping.getNrColors());
			
			int indicesStartByte = 24;
			int lineIndicesStartByte = indicesStartByte + geometryMapping.getNrIndices() * 4;
			int originalLineIndicesStartByte = lineIndicesStartByte;
			int indicesMappingStartByte = generateLineRenders ? (lineIndicesStartByte + geometryMapping.getNrLineIndices() * 4) : lineIndicesStartByte;
			int verticesStartByte = indicesMappingStartByte + geometryMapping.getNrObjects() * ((44 + (generateLineRenders ? 8 : 0)) + (useUuidAndRid ? 24 : 0)) + geometryMapping.getTotalColorPackSize();
			int normalsStartByte = verticesStartByte + geometryMapping.getNrVertices() * (quantizeVertices ? 2 : 4);
			int endByte = normalsStartByte + (quantizeNormals ? (octEncodeNormals ? geometryMapping.getNrVertices() / 3 * 2 : geometryMapping.getNrVertices()) : geometryMapping.getNrVertices() * 4);
			
			int baseIndex = geometryMapping.getBaseIndex();
			
			for (HashMapVirtualObject info : geometryMapping.keySet()) {
				HashMapVirtualObject data = geometryMapping.get(info);
	//			short cid = (short) data.get("type");
	//			EClass eClass = objectProvider.getEClassForCid(cid);
				
				DoubleBuffer transformation = ByteBuffer.wrap((byte[]) info.eGet(info.eClass().getEStructuralFeature("transformation"))).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer();
				double[] ms = new double[16];
				for (int i=0; i<16; i++) {
					ms[i] = transformation.get();
				}
				
				AbstractHashMapVirtualObject indicesBuffer = data.getDirectFeature(GeometryPackage.eINSTANCE.getGeometryData_Indices());
				byte[] normals = null;
				byte[] vertices = null;
				AbstractHashMapVirtualObject normalsBuffer = data.getDirectFeature(GeometryPackage.eINSTANCE.getGeometryData_Normals());
				normals = (byte[]) normalsBuffer.get("data");
				AbstractHashMapVirtualObject verticesBuffer = data.getDirectFeature(GeometryPackage.eINSTANCE.getGeometryData_Vertices());
				vertices = (byte[]) verticesBuffer.get("data");
				IntBuffer indices = ByteBuffer.wrap((byte[]) indicesBuffer.get("data")).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
	
				ByteBuffer vertexByteBuffer = ByteBuffer.wrap(vertices).order(ByteOrder.LITTLE_ENDIAN);
				ByteBuffer normalsByteBuffer = ByteBuffer.wrap(normals).order(ByteOrder.LITTLE_ENDIAN);

				AbstractHashMapVirtualObject lineIndicesBuffer = data.getDirectFeature(GeometryPackage.eINSTANCE.getGeometryData_LineIndices());
				byte[] lineIndicesBytes = null;
				if (lineIndicesBuffer != null) {
					lineIndicesBytes = (byte[]) lineIndicesBuffer.get("data");
				}
				
				Long oid = (Long)info.get("ifcProductOid");
				buffer.putLong(indicesMappingStartByte, oid);
				indicesMappingStartByte += 8;
				if (useUuidAndRid) {
					buffer.position(indicesMappingStartByte);
					byte[] bytes = (byte[])info.get("ifcProductUuid");
					buffer.put(bytes);
					indicesMappingStartByte += 16;
					int pid = (int)info.get("ifcProductPid");
					buffer.putInt(indicesMappingStartByte, pid);
					indicesMappingStartByte += 4;
					int rid = info.getRid();// (int)info.get("ifcProductRid");
					buffer.putInt(indicesMappingStartByte, rid);
					indicesMappingStartByte += 4;
				}
				
				// Start index for object
				int indicesStart = (indicesStartByte - 24) / 4;
				buffer.putInt(indicesMappingStartByte, indicesStart);
				indicesMappingStartByte += 4;
				
				// Start line index for object
				buffer.putInt(indicesMappingStartByte, (lineIndicesStartByte - originalLineIndicesStartByte) / 4);
				indicesMappingStartByte += 4;

				// Nr of indices
				buffer.putInt(indicesMappingStartByte, indices.capacity());
				indicesMappingStartByte += 4;
				
				// Nr of line indices
				buffer.putInt(indicesMappingStartByte, lineIndicesBytes == null ? 0 :lineIndicesBytes.length / 4);
				indicesMappingStartByte += 4;

				// Nr of vertices
				buffer.putInt(indicesMappingStartByte, vertices.length / 8);
				indicesMappingStartByte += 4;

				int minIndex = -1;
				int maxIndex = -1;
				
				for (int i=0; i<indices.capacity(); i++) {
					int index = indices.get();
					
					int modifiedIndex = baseIndex + index + (vertexPosition / 3);
					if (minIndex == -1 || modifiedIndex < minIndex) {
						minIndex = modifiedIndex;
					}
					if (maxIndex == -1 || modifiedIndex > maxIndex) {
						maxIndex = modifiedIndex;
					}
					
					buffer.putInt(indicesStartByte, modifiedIndex);
					indicesStartByte += 4;
				}
				
				int minLineIndex = -1;
				int maxLineIndex = -1;
				
				if (generateLineRenders) {
					IntBuffer lineIndicesInt = ByteBuffer.wrap(lineIndicesBytes).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
					
					for (int i=0; i<lineIndicesInt.capacity(); i++) {
						int index = lineIndicesInt.get();
						
						int modifiedIndex = baseIndex + index + (vertexPosition / 3);
						if (minLineIndex == -1 || modifiedIndex < minLineIndex) {
							minLineIndex = modifiedIndex;
						}
						if (maxLineIndex == -1 || modifiedIndex > maxLineIndex) {
							maxLineIndex = modifiedIndex;
						}
						
						buffer.putInt(lineIndicesStartByte, modifiedIndex);
						lineIndicesStartByte += 4;
					}
				}
				
				buffer.putInt(indicesMappingStartByte, minIndex);
				indicesMappingStartByte += 4;
				buffer.putInt(indicesMappingStartByte, maxIndex);
				indicesMappingStartByte += 4;

				if (generateLineRenders) {
					buffer.putInt(indicesMappingStartByte, minLineIndex);
					indicesMappingStartByte += 4;
					buffer.putInt(indicesMappingStartByte, maxLineIndex);
					indicesMappingStartByte += 4;
				}
				
				float density = (float) info.get("density");
				buffer.putFloat(indicesMappingStartByte, density);
				indicesMappingStartByte += 4;
				
				HashMapVirtualObject colorPack = (HashMapVirtualObject) data.getDirectFeature(GeometryPackage.eINSTANCE.getGeometryData_ColorPack());
				byte[] colorPackData = colorPack == null ? null : (byte[]) colorPack.eGet(GeometryPackage.eINSTANCE.getColorPack_Data());
				if (colorPackData == null || colorPackData.length == 0) {
					buffer.putInt(indicesMappingStartByte, 0);
					indicesMappingStartByte += 4;
				} else {
					int colorPackSize = colorPackData.length / 8;
					buffer.putInt(indicesMappingStartByte, colorPackSize);
					indicesMappingStartByte += 4;
					buffer.position(indicesMappingStartByte);
					buffer.put(colorPackData);
					indicesMappingStartByte += colorPackData.length;
				}

				double[] in = new double[4];
				double[] vertex = new double[4];
				double[] result = new double[4];
				in[3] = 1;
				vertex[3] = 1;
				int nrPos = vertexByteBuffer.capacity() / 8;
				for (int i=0; i<nrPos; i+=3) {
					in[0] = vertexByteBuffer.getDouble();
					in[1] = vertexByteBuffer.getDouble();
					in[2] = vertexByteBuffer.getDouble();
	
					// Apply the transformation matrix of the object
					Matrix.multiplyMV(vertex, 0, ms, 0, in, 0);
					
					// Possibly convert to mm
					if (projectInfo.getMultiplierToMm() != 1f) {
						vertex[0] = vertex[0] * projectInfo.getMultiplierToMm();
						vertex[1] = vertex[1] * projectInfo.getMultiplierToMm();
						vertex[2] = vertex[2] * projectInfo.getMultiplierToMm();
					}

					// Apply the globalTranslation (usually used to move the model to around 0, 0, 0)
					if (globalTranslationVector != null) {
						Vector.addition(vertex, globalTranslationVector, vertex);
					}
					
					if (quantizeVertices) {
						// Apply quantization matrix
						Matrix.multiplyMV(result, 0, vertexQuantizationMatrix, 0, vertex, 0);
						buffer.putShort(verticesStartByte, (short)result[0]);
						buffer.putShort(verticesStartByte + 2, (short)result[1]);
						buffer.putShort(verticesStartByte + 4, (short)result[2]);
						verticesStartByte += 6;
					} else {
						buffer.putFloat(verticesStartByte, (float) vertex[0]);
						buffer.putFloat(verticesStartByte + 4, (float) vertex[1]);
						buffer.putFloat(verticesStartByte + 8, (float) vertex[2]);
						verticesStartByte += 12;
					}
				}
				
				vertexPosition += nrPos;
				
				if (quantizeNormals) {
					double[] inverted = new double[9];
					double[] transposed = new double[9];
					double[] mat3 = new double[9];
					Matrix3.fromMat4(mat3, ms);
					Matrix3.invert(inverted, mat3);
					Matrix3.transpose(transposed, inverted);
					float[] normalIn = new float[3];
					float[] normal = new float[3];
					
					// We need to apply the inverse transpose
					for (int i=0; i<nrPos; i+=3) {
						normalIn[0] = normalsByteBuffer.getFloat();
						normalIn[1] = normalsByteBuffer.getFloat();
						normalIn[2] = normalsByteBuffer.getFloat();
						
						// Apply the transformation matrix of the object
						Matrix3.multiplyMV(normal, normalIn, transposed);
						Vector3D.normalize(normal, normal);

						normalizeNormal(normal);
						
						if (octEncodeNormals) {
							writeNormalToOct(buffer, normalsStartByte, normal);
							normalsStartByte += 2;
						} else {
							buffer.put(normalsStartByte, (byte)(normal[0] * 127));
							buffer.put(normalsStartByte + 1, (byte)( normal[1] * 127));
							buffer.put(normalsStartByte + 2, (byte)(normal[2] * 127));
							normalsStartByte += 3;
						}
					}
				} else {
					double[] inverted = new double[9];
					double[] transposed = new double[9];
					double[] mat3 = new double[9];
					Matrix3.fromMat4(mat3, ms);
					Matrix3.invert(inverted, mat3);
					Matrix3.transpose(transposed, inverted);
					float[] normalIn = new float[3];
					float[] normal = new float[3];
					
					// We need to apply the inverse transpose
					for (int i=0; i<nrPos; i+=3) {
						normalIn[0] = normalsByteBuffer.getFloat();
						normalIn[1] = normalsByteBuffer.getFloat();
						normalIn[2] = normalsByteBuffer.getFloat();
						
						// Apply the transformation matrix of the object
						Matrix3.multiplyMV(normal, normalIn, transposed);
						Vector3D.normalize(normal, normal);
						
						buffer.putFloat(normalsStartByte, normal[0]);
						buffer.putFloat(normalsStartByte + 4, normal[1]);
						buffer.putFloat(normalsStartByte + 8, normal[2]);
						
						normalsStartByte += 12;
					}
				}
			}
			if (normalsStartByte != endByte) {
				LOGGER.error("normalsStartByte != endByte (" + normalsStartByte + ", " + endByte + ")");
			}
			serializerDataOutputStream.write(buffer.array());
			serializerDataOutputStream.align8();
			return geometryMainBuffer.hasNextReadBuffer() || geometryBuffer.hasNextGeometryMapping();
		} catch (IndexOutOfBoundsException e) {
			throw e;
		}
	}

	private void normalizeNormal(float[] normal) {
		for (int i=0; i<3; i++) {
			if (normal[i] > -0.0001 && normal[i] < 0.0001) {
				normal[i] = 0;
			}
		}
	}

	private void reorderForLineRendering(IntBuffer indices, DoubleBuffer vertices, FloatBuffer normals) {
		Mesh mesh = new Mesh(indices, vertices, normals);
		Mesh newMesh = mesh.copy();
		MeshChanger meshChanger = new MeshChanger(mesh, newMesh);
		
		Map<LineSegment, LineSegment> lineSegments = new HashMap<>();
		for (int i=0; i<indices.capacity(); i+=3) {
			int[] triangle = new int[] {indices.get(), indices.get(), indices.get()};
			for (int j=0; j<3; j++) {
				LineSegment lineSegment = new LineSegment(i / 3, triangle[j], triangle[(j+1)%3]);
				if (lineSegments.containsKey(lineSegment)) {
					lineSegments.remove(lineSegment);
				} else {
					lineSegments.put(lineSegment, lineSegment);
				}
			}
		}
		
		for (LineSegment lineSegment : lineSegments.keySet()) {
			lineSegment.getIndex1();
		}
	}

	private float signNotZero(float in) {
		return in >= 0f ? 1 : -1;
	}
	
//	function octEncodeVec3(array, i, xfunc, yfunc) {
//        var x = array[i    ] / (Math.abs(array[i]) + Math.abs(array[i + 1]) + Math.abs(array[i + 2]));
//        var y = array[i + 1] / (Math.abs(array[i]) + Math.abs(array[i + 1]) + Math.abs(array[i + 2]));
//
//        if (array[i + 2] < 0) {
//            var tempx = x;
//            var tempy = y;
//            tempx = (1 - Math.abs(y)) * (x >= 0 ? 1 : -1);
//            tempy = (1 - Math.abs(x)) * (y >= 0 ? 1 : -1);
//            x = tempx;
//            y = tempy;
//        }
//
//        return new Int8Array([
//            Math[xfunc](x * 127.5 + (x < 0 ? -1 : 0)),
//            Math[yfunc](y * 127.5 + (y < 0 ? -1 : 0))
//        ]);
//
//    }
	
	private void writeNormalToOct(ByteBuffer buffer, int normalsStartByte, float[] normal) {
		float x = Math.abs(normal[0]) + Math.abs(normal[1]) + Math.abs(normal[2]);
		float[] p = new float[] {normal[0] / x, normal[1] / x};
		
		if (normal[2] <= 0f) {
			float a = (1f - Math.abs(p[0])) * signNotZero(p[0]);
			float b = (1f - Math.abs(p[1])) * signNotZero(p[1]);
			p = new float[]{a, b};
		}

		byte a = (byte)(p[0] * 127f);
		buffer.put(normalsStartByte, a);
		byte b = (byte)(p[1] * 127f);
		buffer.put(normalsStartByte + 1, b);
		
//		System.out.println(normal[0] + ", " + normal[1] + ", " + normal[2]);
//		if (lastNormal == null || !Arrays.equals(this.lastNormal, normal)) {
//			System.out.println(normal[0] + ", " + normal[1] + ", " + normal[2]);
//			System.out.println(a + ", " + b);
//			if (this.lastNormal == null) {
//				this.lastNormal = new float[3];
//			}
//			System.arraycopy(normal, 0, this.lastNormal, 0, 3);
//		}
	}

	private void load() throws SerializerException {
//		long start = System.nanoTime();
		if (reportProgress) {
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
			} catch (IOException e) {
				e.printStackTrace();
			} catch (QueryException e) {
				e.printStackTrace();
			}
		}
		try {
			this.next = objectProvider.next();
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
		
		serializerDataOutputStream.writeFloat(projectInfo.getMultiplierToMm());
		serializerDataOutputStream.align8();

		// TODO These are known to be wrong for multi-roid queries
		SVector3f minBounds = projectInfo.getMinBounds();
		SVector3f maxBounds = projectInfo.getMaxBounds();
		
		if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
			serializerDataOutputStream.writeDouble(minBounds.getX() * projectInfo.getMultiplierToMm());
			serializerDataOutputStream.writeDouble(minBounds.getY() * projectInfo.getMultiplierToMm());
			serializerDataOutputStream.writeDouble(minBounds.getZ() * projectInfo.getMultiplierToMm());
			serializerDataOutputStream.writeDouble(maxBounds.getX() * projectInfo.getMultiplierToMm());
			serializerDataOutputStream.writeDouble(maxBounds.getY() * projectInfo.getMultiplierToMm());
			serializerDataOutputStream.writeDouble(maxBounds.getZ() * projectInfo.getMultiplierToMm());
		} else {
			serializerDataOutputStream.writeDouble(minBounds.getX());
			serializerDataOutputStream.writeDouble(minBounds.getY());
			serializerDataOutputStream.writeDouble(minBounds.getZ());
			serializerDataOutputStream.writeDouble(maxBounds.getX());
			serializerDataOutputStream.writeDouble(maxBounds.getY());
			serializerDataOutputStream.writeDouble(maxBounds.getZ());
		}
	}

	private boolean writeData() throws IOException, SerializerException {
		if (next == null) {
			return false;
		}
		boolean wroteSomething = false;
		while (!wroteSomething && next != null) {
			if (GeometryPackage.eINSTANCE.getGeometryInfo() == next.eClass()) {
				writeGeometryInfo(next);
				wroteSomething = true;
			} else if (GeometryPackage.eINSTANCE.getGeometryData() == next.eClass()) {
				wroteSomething = writeGeometryData(next, next.getOid());
			}
			try {
				next = objectProvider.next();
			} catch (BimserverDatabaseException e) {
				LOGGER.error("", e);
			}
		}
		return next != null;
	}

	private void writeGeometryInfo(HashMapVirtualObject info) throws IOException, SerializerException {
		boolean hasTransparancy = (boolean)info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_HasTransparency());
		long geometryDataId = (long)info.eGet(info.eClass().getEStructuralFeature("data"));
		boolean inPreparedBuffer = false;
		long oid = (long) info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_IfcProductOid());
		if (prepareBuffers && (reusedDataOids == null || !reusedDataOids.contains(geometryDataId))) {
			inPreparedBuffer = true;
			if (oidToGeometryData.containsKey(geometryDataId)) {
				GeometryMainBuffer geometryMainBuffer = getCurrentMainBuffer(hasTransparancy);
				GeometryBuffer currentBuffer = geometryMainBuffer.getCurrentWriteBuffer();
				GeometrySubBuffer geometryMapping = currentBuffer.getCurrentGeometryMapping(true);
				HashMapVirtualObject gd = oidToGeometryData.get(geometryDataId);
				geometryMapping.put(info, gd);
				updateSize(gd, currentBuffer);
			} else {
				dataToGeometryInfo.put(geometryDataId, info);
			}
			writeMinimalGeometryInfo(info);
			return;
		}
		
		byte[] transformation = (byte[]) info.eGet(info.eClass().getEStructuralFeature("transformation"));
		long dataOid = geometryDataId;
		
		serializerDataOutputStream.writeByte(MessageType.GEOMETRY_INFO.getId());
		serializerDataOutputStream.writeByte(inPreparedBuffer ? (byte)1 : (byte)0);
		serializerDataOutputStream.writeLong(oid);
		
		if (useUuidAndRid) {
			serializerDataOutputStream.write((byte[])info.get("ifcProductUuid"));
			serializerDataOutputStream.writeInt((int)info.get("ifcProductPid"));
			serializerDataOutputStream.writeInt(info.getRid());
		}
		
		String type = objectProvider.getEClassForOid(oid).getName();
		serializerDataOutputStream.writeUTF(type);
		int nrColors = (int)info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_NrColors());
		if (nrColors == 0) {
			int nrVertices = (int)info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_NrVertices());
			nrColors = nrVertices / 3 * 4;
		}
		serializerDataOutputStream.writeInt(nrColors);
		serializerDataOutputStream.align8();
		serializerDataOutputStream.ensureExtraCapacity(24);
		serializerDataOutputStream.writeLongUnchecked(info.getRoid());
		serializerDataOutputStream.writeLongUnchecked(info.getOid());
		serializerDataOutputStream.writeLongUnchecked(hasTransparancy ? 1 : 0);
		writeBounds(info);
		
		lastTransformation = ByteBuffer.wrap(transformation);
		lastTransformation.order(ByteOrder.LITTLE_ENDIAN);
		
		ByteBuffer newTransformation = lastTransformation;
		
		// Apply the globalTranslation (usually used to move the model to around 0, 0, 0)
		if (globalTranslationVector != null) {
			DoubleBuffer asDoubleBuffer = lastTransformation.asDoubleBuffer();
			double[] ms = new double[16];
			for (int i=0; i<16; i++) {
				ms[i] = asDoubleBuffer.get();
			}

			double[] tmp = new double[16];
			double[] translationMatrix = new double[16];
			Matrix.setIdentityM(translationMatrix, 0);
			Matrix.translateM(translationMatrix, 0, translationMatrix, 0, globalTranslationVector[0] / projectInfo.getMultiplierToMm(), globalTranslationVector[1] / projectInfo.getMultiplierToMm(), globalTranslationVector[2] / projectInfo.getMultiplierToMm());
			Matrix.multiplyMM(tmp, 0, translationMatrix, 0, ms, 0);
			ms = tmp;

			newTransformation = ByteBuffer.wrap(new byte[16 * 8]).order(ByteOrder.LITTLE_ENDIAN);
			for (double d : ms) {
				newTransformation.putDouble(d);
			}
		}
		
		serializerDataOutputStream.ensureExtraCapacity(((byte[])transformation).length + 8);
		serializerDataOutputStream.write(newTransformation.array());
		serializerDataOutputStream.writeLong(dataOid);
		
		nrObjectsWritten++;
		if (reportProgress) {
			if (progressReporter != null) {
				progressReporter.update(nrObjectsWritten, size);
			}
		}
	}

	private void writeBounds(HashMapVirtualObject info) throws IOException {
		HashMapWrappedVirtualObject bounds = (HashMapWrappedVirtualObject) info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_Bounds());
		HashMapWrappedVirtualObject minBounds = (HashMapWrappedVirtualObject) bounds.eGet(GeometryPackage.eINSTANCE.getBounds_Min());
		HashMapWrappedVirtualObject maxBounds = (HashMapWrappedVirtualObject) bounds.eGet(GeometryPackage.eINSTANCE.getBounds_Max());
		Double minX = (Double) minBounds.eGet("x");
		Double minY = (Double) minBounds.eGet("y");
		Double minZ = (Double) minBounds.eGet("z");
		Double maxX = (Double) maxBounds.eGet("x");
		Double maxY = (Double) maxBounds.eGet("y");
		Double maxZ = (Double) maxBounds.eGet("z");
		
		if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
			minX = minX * projectInfo.getMultiplierToMm();
			minY = minY * projectInfo.getMultiplierToMm();
			minZ = minZ * projectInfo.getMultiplierToMm();
			maxX = maxX * projectInfo.getMultiplierToMm();
			maxY = maxY * projectInfo.getMultiplierToMm();
			maxZ = maxZ * projectInfo.getMultiplierToMm();
		}
		
		serializerDataOutputStream.ensureExtraCapacity(8 * 6);
		serializerDataOutputStream.writeDoubleUnchecked(minX);
		serializerDataOutputStream.writeDoubleUnchecked(minY);
		serializerDataOutputStream.writeDoubleUnchecked(minZ);
		serializerDataOutputStream.writeDoubleUnchecked(maxX);
		serializerDataOutputStream.writeDoubleUnchecked(maxY);
		serializerDataOutputStream.writeDoubleUnchecked(maxZ);
	}

	private void writeMinimalGeometryInfo(HashMapVirtualObject info) throws IOException {
		boolean hasTransparancy = (boolean)info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_HasTransparency());
		long geometryDataId = (long)info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_Data());
		long oid = (long) info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_IfcProductOid());
		
		serializerDataOutputStream.writeByte(MessageType.MINIMAL_GEOMETRY_INFO.getId());
		serializerDataOutputStream.writeLong(oid);
		if (useUuidAndRid) {
			serializerDataOutputStream.write((byte[])info.get("ifcProductUuid"));
			serializerDataOutputStream.writeInt((int)info.get("ifcProductPid"));
			serializerDataOutputStream.writeInt(info.getRid());
		}
		String type = objectProvider.getEClassForOid(oid).getName();
		serializerDataOutputStream.writeUTF(type);
		int nrColors = (int)info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_NrColors());
		if (nrColors == 0) {
			int nrVertices = (int)info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_NrVertices());
			nrColors = nrVertices / 3 * 4;
		}
		serializerDataOutputStream.writeInt(nrColors);
		serializerDataOutputStream.ensureExtraCapacity(32);
		serializerDataOutputStream.writeLongUnchecked(info.getRoid());
		serializerDataOutputStream.writeLongUnchecked(info.getOid());
		serializerDataOutputStream.writeLongUnchecked(hasTransparancy ? 1 : 0);
		serializerDataOutputStream.align8();
		
		writeBounds(info);
		
		serializerDataOutputStream.writeLong(geometryDataId);
		
		nrObjectsWritten++;
		if (reportProgress) {
			if (progressReporter != null) {
				progressReporter.update(nrObjectsWritten, size);
			}
		}		
	}

	private boolean writeGeometryData(HashMapVirtualObject data, long oid) throws IOException, SerializerException {
		// This geometry info is pointing to a not-yet-sent geometry data, so we send that first
		// This way the client can be sure that geometry data is always available when geometry info is received, simplifying bookkeeping
		EStructuralFeature hasTransparencyFeature = data.eClass().getEStructuralFeature("hasTransparency");
		boolean hasTransparancy = (boolean)data.eGet(hasTransparencyFeature);
		
		if (prepareBuffers && (reusedDataOids == null || !reusedDataOids.contains(oid))) {
			oidToGeometryData.put(oid, data);
			if (dataToGeometryInfo.containsKey(oid)) {
				GeometryMainBuffer geometryMainBuffer = getCurrentMainBuffer(hasTransparancy);
				GeometryBuffer currentBuffer = geometryMainBuffer.getCurrentWriteBuffer();
				GeometrySubBuffer geometryMapping = currentBuffer.getCurrentGeometryMapping(true);
				geometryMapping.put(dataToGeometryInfo.get(oid), data);
				updateSize(data, currentBuffer);
			}
			return false;
		}
		
		EStructuralFeature indicesFeature = data.eClass().getEStructuralFeature("indices");
		EStructuralFeature lineIndicesFeature = data.eClass().getEStructuralFeature("lineIndices");
		EStructuralFeature verticesFeature = data.eClass().getEStructuralFeature("vertices");
		EStructuralFeature verticesQuantizedFeature = data.eClass().getEStructuralFeature("verticesQuantized");
		EStructuralFeature normalsFeature = data.eClass().getEStructuralFeature("normals");
		EStructuralFeature normalsQuantizedFeature = data.eClass().getEStructuralFeature("normalsQuantized");
		EStructuralFeature colorsFeature = data.eClass().getEStructuralFeature("colorsQuantized");
		EStructuralFeature colorFeature = data.eClass().getEStructuralFeature("color");
		EStructuralFeature mostUsedColorFeature = data.eClass().getEStructuralFeature("mostUsedColor");

		AbstractHashMapVirtualObject indicesBuffer = data.getDirectFeature(indicesFeature);
		byte[] normals = null;
		byte[] normalsQuantized = null;
		byte[] vertices = null;
		byte[] verticesQuantized = null;
		AbstractHashMapVirtualObject quantizedNormalsBuffer = data.getDirectFeature(normalsQuantizedFeature);
		if (quantizeNormals && quantizedNormalsBuffer != null) {
			normalsQuantized = (byte[]) quantizedNormalsBuffer.get("data");
		} else {
			AbstractHashMapVirtualObject normalsBuffer = data.getDirectFeature(normalsFeature);
			normals = (byte[]) normalsBuffer.get("data");
		}
		AbstractHashMapVirtualObject quantizedVerticesBuffer = data.getDirectFeature(verticesQuantizedFeature);
		if (quantizeVertices && quantizedVerticesBuffer != null) {
			verticesQuantized = (byte[]) quantizedVerticesBuffer.get("data");
		} else {
			AbstractHashMapVirtualObject verticesBuffer = data.getDirectFeature(verticesFeature);
			vertices = (byte[]) verticesBuffer.get("data");
		}
		AbstractHashMapVirtualObject colorsBuffer = data.getDirectFeature(colorsFeature);
		HashMapWrappedVirtualObject color = (HashMapWrappedVirtualObject)data.eGet(colorFeature);
		HashMapWrappedVirtualObject mostUsedColor = (HashMapWrappedVirtualObject)data.eGet(mostUsedColorFeature);
		
		byte[] indices = (byte[]) indicesBuffer.get("data");
		byte[] colors = null;
		if (colorsBuffer != null) {
			colors = (byte[]) colorsBuffer.get("data");			
		}

		int totalNrIndices = indices.length / 4;
		int maxIndexValues = 16389;

		if (splitGeometry && totalNrIndices > maxIndexValues) {
			serializerDataOutputStream.writeByte(MessageType.GEOMETRY_TRIANGLES_PARTED.getId());
			serializerDataOutputStream.writeInt((int) data.get("reused"));
			short cid = (short) data.get("type");
			String type = objectProvider.getEClassForCid(cid).getName();
			serializerDataOutputStream.writeUTF(type);
			serializerDataOutputStream.align8();
			serializerDataOutputStream.writeLong(hasTransparancy ? 1 : 0);
			serializerDataOutputStream.writeLong(data.getOid());
			
			// Split geometry, this algorithm - for now - just throws away all the reuse of vertices that might be there
			// Also, although usually the vertices buffers are too large, this algorithm is based on the indices, so we
			// probably are not cramming as much data as we can in each "part", but that's not really a problem I think

			int nrParts = (totalNrIndices + maxIndexValues - 1) / maxIndexValues;
			serializerDataOutputStream.writeInt(nrParts);

			ByteBuffer indicesByteBuffer = ByteBuffer.wrap(indices);
			indicesByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			IntBuffer indicesIntBuffer = indicesByteBuffer.asIntBuffer();

			ByteBuffer vertexBuffer = ByteBuffer.wrap(vertices);
			vertexBuffer.order(ByteOrder.LITTLE_ENDIAN);
			DoubleBuffer verticesDoubleBuffer = vertexBuffer.asDoubleBuffer();
			
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
				
				serializerDataOutputStream.align4();
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
					
					serializerDataOutputStream.writeFloatUnchecked((float) verticesDoubleBuffer.get(oldIndex1 * 3));
					serializerDataOutputStream.writeFloatUnchecked((float) verticesDoubleBuffer.get(oldIndex1 * 3 + 1));
					serializerDataOutputStream.writeFloatUnchecked((float) verticesDoubleBuffer.get(oldIndex1 * 3 + 2));
					serializerDataOutputStream.writeFloatUnchecked((float) verticesDoubleBuffer.get(oldIndex2 * 3));
					serializerDataOutputStream.writeFloatUnchecked((float) verticesDoubleBuffer.get(oldIndex2 * 3 + 1));
					serializerDataOutputStream.writeFloatUnchecked((float) verticesDoubleBuffer.get(oldIndex2 * 3 + 2));
					serializerDataOutputStream.writeFloatUnchecked((float) verticesDoubleBuffer.get(oldIndex3 * 3));
					serializerDataOutputStream.writeFloatUnchecked((float) verticesDoubleBuffer.get(oldIndex3 * 3 + 1));
					serializerDataOutputStream.writeFloatUnchecked((float) verticesDoubleBuffer.get(oldIndex3 * 3 + 2));
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
				if (colors != null && color == null) {
					ByteBuffer materialsByteBuffer = ByteBuffer.wrap(colors);
					
					serializerDataOutputStream.writeInt(nrVertices * 4 / 3);
					serializerDataOutputStream.ensureExtraCapacity(16 * (upto - part * maxIndexValues));
					for (int i=part * maxIndexValues; i<upto; i+=3) {
						int oldIndex1 = indicesIntBuffer.get(i);
						int oldIndex2 = indicesIntBuffer.get(i+1);
						int oldIndex3 = indicesIntBuffer.get(i+2);
						
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex1 * 4)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex1 * 4 + 1)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex1 * 4 + 2)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex1 * 4 + 3)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex2 * 4)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex2 * 4 + 1)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex2 * 4 + 2)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex2 * 4 + 3)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex3 * 4)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex3 * 4 + 1)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex3 * 4 + 2)) / 255f);
						serializerDataOutputStream.writeFloatUnchecked(UnsignedBytes.toInt(materialsByteBuffer.get(oldIndex3 * 4 + 3)) / 255f);
					}
				} else {
					// No materials used
					serializerDataOutputStream.writeInt(0);
				}
			}
		} else {
			serializerDataOutputStream.writeByte(MessageType.GEOMETRY_TRIANGLES.getId());
			int reused = (int) data.get("reused");
			serializerDataOutputStream.writeInt(reused);

			short cid = (short) data.get("type");
			String type = objectProvider.getEClassForCid(cid).getName();
			serializerDataOutputStream.writeUTF(type);
			serializerDataOutputStream.align8();
			long roid = data.getRoid();
			long croid = data.getCroid();
			serializerDataOutputStream.writeLong(roid);
			serializerDataOutputStream.writeLong(croid);

			serializerDataOutputStream.writeLong(hasTransparancy ? 1 : 0);
			serializerDataOutputStream.writeLong(oid);
			
			ByteBuffer indicesByteBuffer = ByteBuffer.wrap(indices);
			indicesByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
			serializerDataOutputStream.writeInt(indicesByteBuffer.capacity() / 4);
			if (useSmallInts) {
				IntBuffer intBuffer = indicesByteBuffer.asIntBuffer();
				serializerDataOutputStream.ensureExtraCapacity(intBuffer.capacity() * 2);
				for (int i=0; i<intBuffer.capacity(); i++) {
					serializerDataOutputStream.writeShortUnchecked((short) intBuffer.get());
				}
				serializerDataOutputStream.align4();
			} else {
				serializerDataOutputStream.write(indicesByteBuffer.array());
			}
	
			if (generateLineRenders) {
				AbstractHashMapVirtualObject lineIndicesBuffer = data.getDirectFeature(lineIndicesFeature);
				byte[] lineIndices = (byte[]) lineIndicesBuffer.get("data");
				ByteBuffer lineIndicesByteBuffer = ByteBuffer.wrap(lineIndices);
				lineIndicesByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
				serializerDataOutputStream.writeInt(lineIndicesByteBuffer.capacity() / 4);
				if (useSmallInts) {
					IntBuffer intBuffer = lineIndicesByteBuffer.asIntBuffer();
					serializerDataOutputStream.ensureExtraCapacity(intBuffer.capacity() * 2);
					for (int i=0; i<intBuffer.capacity(); i++) {
						serializerDataOutputStream.writeShortUnchecked((short) intBuffer.get());
					}
					serializerDataOutputStream.align4();
				} else {
					serializerDataOutputStream.write(lineIndicesByteBuffer.array());
				}
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

			if (quantizeVertices) {
				if (verticesQuantized != null && normalizeUnitsToMM) {
					serializerDataOutputStream.writeInt(verticesQuantized.length / 2);
					serializerDataOutputStream.write(verticesQuantized);
					serializerDataOutputStream.align8();
				} else {
					ByteBuffer vertexByteBuffer = ByteBuffer.wrap(vertices);
					int nrPos = vertexByteBuffer.capacity() / 8;
					serializerDataOutputStream.writeInt(nrPos);

					vertexByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
					serializerDataOutputStream.ensureExtraCapacity(vertexByteBuffer.capacity() * 2 / 8);
					double[] vertex = new double[4];
					double[] result = new double[4];
					vertex[3] = 1;
					for (int i=0; i<nrPos; i+=3) {
						vertex[0] = vertexByteBuffer.getDouble();
						vertex[1] = vertexByteBuffer.getDouble();
						vertex[2] = vertexByteBuffer.getDouble();
						
						if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
							vertex[0] = vertex[0] * (double)projectInfo.getMultiplierToMm();
							vertex[1] = vertex[1] * (double)projectInfo.getMultiplierToMm();
							vertex[2] = vertex[2] * (double)projectInfo.getMultiplierToMm();
						}
						
//						float[] matrix = vertexQuantizationMatrices.get(croid);
						double[] matrix = vertexQuantizationMatrix;
						if (matrix == null) {
							LOGGER.error("Missing quant matrix for " + croid);
							return false;
						}
						Matrix.multiplyMV(result, 0, matrix, 0, vertex, 0);
						
						serializerDataOutputStream.writeShortUnchecked((short)result[0]);
						serializerDataOutputStream.writeShortUnchecked((short)result[1]);
						serializerDataOutputStream.writeShortUnchecked((short)result[2]);
					}
					serializerDataOutputStream.align8();
				}
			} else {
				ByteBuffer vertexByteBuffer = ByteBuffer.wrap(vertices).order(ByteOrder.LITTLE_ENDIAN);
				int nrPos = vertexByteBuffer.capacity() / 8;
				serializerDataOutputStream.writeInt(nrPos);
				if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
					vertexByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
					serializerDataOutputStream.ensureExtraCapacity(vertexByteBuffer.capacity() * 6 / 8); // TODO unchecked
					double[] vertex = new double[3];
					for (int i=0; i<nrPos; i+=3) {
						vertex[0] = vertexByteBuffer.getDouble();
						vertex[1] = vertexByteBuffer.getDouble();
						vertex[2] = vertexByteBuffer.getDouble();
						vertex[0] = vertex[0] * (double)projectInfo.getMultiplierToMm();
						vertex[1] = vertex[1] * (double)projectInfo.getMultiplierToMm();
						vertex[2] = vertex[2] * (double)projectInfo.getMultiplierToMm();
						
						serializerDataOutputStream.writeFloatUnchecked((float) vertex[0]);
						serializerDataOutputStream.writeFloatUnchecked((float) vertex[1]);
						serializerDataOutputStream.writeFloatUnchecked((float) vertex[2]);
					}
				} else {
					DoubleBuffer vertexDoubleBuffer = vertexByteBuffer.asDoubleBuffer();
					for (int i=0; i<vertexDoubleBuffer.capacity(); i++) {
						serializerDataOutputStream.writeFloat((float) vertexDoubleBuffer.get(i));
					}
				}
			}
			
			if (quantizeNormals && normalsQuantized != null) {
				if (octEncodeNormals) {
					float[] floatNormal = new float[3];
					byte[] oct = new byte[2];
					serializerDataOutputStream.writeInt((normalsQuantized.length / 3) * 2);
					for (int i=0; i<normalsQuantized.length; i+=3) {
						floatNormal[0] = normalsQuantized[i] / 127;
						floatNormal[1] = normalsQuantized[i + 1] / 127;
						floatNormal[2] = normalsQuantized[i + 2] / 127;
						octEncodeFloat(floatNormal, oct);
						serializerDataOutputStream.write(oct[0]);
						serializerDataOutputStream.write(oct[1]);
					}
					serializerDataOutputStream.align8();
				} else {
					serializerDataOutputStream.ensureExtraCapacity(normalsQuantized.length + 4);
					serializerDataOutputStream.writeInt(normalsQuantized.length);
					serializerDataOutputStream.write(normalsQuantized);
					serializerDataOutputStream.align8();
				}
			} else {
				serializerDataOutputStream.writeInt(normals.length / 4);
				serializerDataOutputStream.write(normals);
			}
			
			if (useSingleColors) {
				serializerDataOutputStream.writeInt(0);
			} else {
				if (colors == null) {
					// We need to generate them
					if (color == null) {
						serializerDataOutputStream.writeInt(0);
						return true;
					} else {
						serializerDataOutputStream.writeInt(0);
						return true;
					}
//						int nrVertices = 0;
//						if (vertices == null) {
//							nrVertices = verticesQuantized.length / 6;
//						} else {
//							nrVertices = vertices.length / 12;
//						}
//						serializerDataOutputStream.writeInt(nrVertices * 4);
//						serializerDataOutputStream.ensureExtraCapacity(nrVertices * 4 * (quantizeColors ? 1 : 4));
//						byte[] quantizedColor = new byte[4];
//						quantizedColor[0] = UnsignedBytes.checkedCast((long)((float)color.eGet("x") * 255f));
//						quantizedColor[1] = UnsignedBytes.checkedCast((long)((float)color.eGet("y") * 255f));
//						quantizedColor[2] = UnsignedBytes.checkedCast((long)((float)color.eGet("z") * 255f));
//						quantizedColor[3] = UnsignedBytes.checkedCast((long)((float)color.eGet("w") * 255f));
//						for (int i=0; i<nrVertices; i++) {
//							if (quantizeColors) {
//								serializerDataOutputStream.writeUnchecked(quantizedColor, 0, 4);
//							} else {
//								serializerDataOutputStream.writeFloatUnchecked((float) color.eGet("x"));
//								serializerDataOutputStream.writeFloatUnchecked((float) color.eGet("y"));
//								serializerDataOutputStream.writeFloatUnchecked((float) color.eGet("z"));
//								serializerDataOutputStream.writeFloatUnchecked((float) color.eGet("w"));
//							}
//						}
				} else {
					ByteBuffer materialsByteBuffer = ByteBuffer.wrap(colors);
					serializerDataOutputStream.writeInt(materialsByteBuffer.capacity());
					if (quantizeColors) {
						serializerDataOutputStream.write(materialsByteBuffer.array());
					} else {
						serializerDataOutputStream.ensureExtraCapacity(materialsByteBuffer.capacity() * 4);
						for (int i=0; i<materialsByteBuffer.capacity(); i++) {
							byte b = materialsByteBuffer.get(i);
							float f = UnsignedBytes.toInt(b) / 255f;
							serializerDataOutputStream.writeFloatUnchecked(f);
						}
					}
				}
			}
		}
		return true;
	}
	
	private void octEncodeFloat(float[] in, byte[] out) {
		float x = Math.abs(in[0]) + Math.abs(in[1]) + Math.abs(in[2]);
		float[] p = new float[] {in[0] / x, in[1] / x};
		
		if (in[2] <= 0f) {
			float a = (1f - Math.abs(p[0])) * signNotZero(p[0]);
			float b = (1f - Math.abs(p[1])) * signNotZero(p[1]);
			p = new float[]{a, b};
		}

		out[0] = (byte)(p[0] * 127f);
		out[1] = (byte)(p[1] * 127f);
	}

	private void updateSize(HashMapVirtualObject data, GeometryBuffer geometryBuffer) {
		int nrIndices = (int)data.eGet(GeometryPackage.eINSTANCE.getGeometryData_NrIndices());
		int nrLineIndices = (int)data.eGet(GeometryPackage.eINSTANCE.getGeometryData_NrLineIndices());
		int nrVertices = (int)data.eGet(GeometryPackage.eINSTANCE.getGeometryData_NrVertices());

		HashMapVirtualObject colorPack = (HashMapVirtualObject) data.getDirectFeature(GeometryPackage.eINSTANCE.getGeometryData_ColorPack());
		byte[] colorPackData = colorPack == null ? null : (byte[]) colorPack.eGet(GeometryPackage.eINSTANCE.getColorPack_Data());
		
		GeometrySubBuffer currentGeometryMapping = geometryBuffer.getCurrentGeometryMapping(false);
		
		currentGeometryMapping.incNrIndices(nrIndices);
		if (generateLineRenders) {
			currentGeometryMapping.incNrLineIndices(nrLineIndices);
		}
		currentGeometryMapping.incNrVertices(nrVertices);
		currentGeometryMapping.incNrColors(colorPackData == null ? 0 : colorPackData.length);
		currentGeometryMapping.incTotalColorPackSize(colorPackData == null ? 0 : colorPackData.length);
		currentGeometryMapping.incNrObjects();
		
		currentGeometryMapping.incPreparedSize( 
			nrIndices * 4 + // Each index number uses 4 bytes
			(generateLineRenders ? (nrLineIndices * 4) : 0) + // Each line index number uses 4 bytes as well
			nrVertices * (quantizeVertices ? 2 : 4) + // Each vertex uses 2 bytes (quantized) per number
			(quantizeNormals ? (octEncodeNormals ? ((nrVertices / 3) * 2) : nrVertices) : nrVertices * 4) + // Each vertex uses 2 bytes per normal for (oct-encoded) normals
			4 + // Density
			(colorPackData == null ? 0 : colorPackData.length) + //
			40 + (generateLineRenders ? 8 : 0) + // 8 for the oid, 4 for the startIndex, 4 for the nrIndices
			(useUuidAndRid ? 24 : 0)); // 16 bytes for UUID and 4 for rid
	}
	
	private GeometryMainBuffer getCurrentMainBuffer() {
		if (mode == Mode.PREPARED_BUFFER_TRANSPARENT) {
			return transparentGeometryBuffer;
		} else if (mode == Mode.PREPARED_BUFFER_OPAQUE) {
			return opaqueGeometryBuffer;
		}
		return null;
	}

	private GeometryMainBuffer getCurrentMainBuffer(boolean hasTransparency) {
		if (hasTransparency) {
			return transparentGeometryBuffer;
		} else {
			return opaqueGeometryBuffer;
		}
	}

	@SuppressWarnings("unused")
	private boolean inside(Point a, double[] bounds) {
		if (a.getX() < bounds[0] || a.getY() < bounds[1] || a.getZ() < bounds[2] || a.getX() > bounds[3] || a.getY() > bounds[4] || a.getZ() > bounds[5]) {
			return false;
		}
		return true;
	}

	@Override
	public void close() {
	}
}