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
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.bimserver.serializers.binarygeometry.clipping.Edge;
import org.bimserver.serializers.binarygeometry.clipping.Point;
import org.bimserver.serializers.binarygeometry.clipping.PolygonClipping;
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
	 */
	
	private static final byte FORMAT_VERSION = 15;
	
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

	private boolean splitGeometry = true;
	private boolean useSingleColors = false;
	private boolean quantizeNormals = false;
	private boolean quantizeVertices = false;
	private boolean quantizeColors = false;
	private boolean normalizeUnitsToMM = false;
	private boolean useSmallInts = true;
	private boolean splitTriangles = false;
	private boolean reportProgress;
	private Map<Long, float[]> vertexQuantizationMatrices;
	
	private Mode mode = Mode.LOAD;
	private long splitCounter = 0;
	private ObjectProvider objectProvider;
	private ProjectInfo projectInfo;
	private SerializerDataOutputStream serializerDataOutputStream;
	private HashMapVirtualObject next;
	private ProgressReporter progressReporter;
	private int nrObjectsWritten;
	private int size;

	private double[] bounds;

	private ArrayList<Edge> clippingPlane;

	private PolygonClipping polygonClipping;

	private ByteBuffer lastTransformation;

	private DoubleBuffer asDoubleBuffer;

	private double[] ms;

	private double[] inverse = new double[16];

	private Map<Long, Integer> dataOidsWritten = new HashMap<>();

	private JsonNode inBoundingBox;

	private boolean excludeOctants;


//	private Bounds modelBounds;
//	private Bounds modelBoundsUntranslated;

	@Override
	public void init(ObjectProvider objectProvider, ProjectInfo projectInfo, PluginManagerInterface pluginManager, PackageMetaData packageMetaData) throws SerializerException {
		this.objectProvider = objectProvider;
		this.projectInfo = projectInfo;
		ObjectNode queryNode = objectProvider.getQueryNode();
		if (queryNode.has("loaderSettings")) {
			ObjectNode geometrySettings = (ObjectNode) queryNode.get("loaderSettings");
			
			useSingleColors = geometrySettings.has("useObjectColors") && geometrySettings.get("useObjectColors").asBoolean();
			splitGeometry = geometrySettings.has("splitGeometry") && geometrySettings.get("splitGeometry").asBoolean();
			quantizeNormals = geometrySettings.has("quantizeNormals") && geometrySettings.get("quantizeNormals").asBoolean();
			quantizeVertices = geometrySettings.has("quantizeVertices") && geometrySettings.get("quantizeVertices").asBoolean();
			quantizeColors = geometrySettings.has("quantizeColors") && geometrySettings.get("quantizeColors").asBoolean();
			normalizeUnitsToMM = geometrySettings.has("normalizeUnitsToMM") && geometrySettings.get("normalizeUnitsToMM").asBoolean();
			reportProgress = !geometrySettings.has("reportProgress") || geometrySettings.get("reportProgress").asBoolean(); // default is true for backwards compat
			useSmallInts = !geometrySettings.has("useSmallInts") || geometrySettings.get("useSmallInts").asBoolean(); // default is true for backwards compat
			splitTriangles = geometrySettings.has("splitTriangles") && geometrySettings.get("splitTriangles").asBoolean();
			ArrayNode queries = (ArrayNode) objectProvider.getQueryNode().get("queries");
			ObjectNode query = (ObjectNode) queries.get(0);
			if (query.has("inBoundingBox")) {
				inBoundingBox = query.get("inBoundingBox");
				bounds = new double[6];
				bounds[0] = inBoundingBox.get("x").asDouble();
				bounds[1] = inBoundingBox.get("y").asDouble();
				bounds[2] = inBoundingBox.get("z").asDouble();
				bounds[3] = inBoundingBox.get("width").asDouble();
				bounds[4] = inBoundingBox.get("height").asDouble();
				bounds[5] = inBoundingBox.get("depth").asDouble();
				
				if (inBoundingBox.has("excludeOctants")) {
					this.excludeOctants = true;
				}
			}
			if (splitTriangles) {
				
				clippingPlane = new ArrayList<>();
				Point a = new Point(bounds[0], bounds[1], bounds[2]);
				Point b = new Point(bounds[0] + bounds[3], bounds[1], bounds[2]);
				Point c = new Point(bounds[0] + bounds[3], bounds[1] + bounds[4], bounds[2]);
				Point d = new Point(bounds[0], bounds[1] + bounds[4] + bounds[4], bounds[2]);
				Point e = new Point(bounds[0], bounds[1], bounds[2] + bounds[5]);
				Point f = new Point(bounds[0] + bounds[3], bounds[1], bounds[2] + bounds[5]);
				Point g = new Point(bounds[0] + bounds[3], bounds[1] + bounds[4], bounds[2] + bounds[5]);
				Point h = new Point(bounds[0], bounds[1] + bounds[4] + bounds[4], bounds[2] + bounds[5]);
				
				clippingPlane.add(new Edge(a, b));
				clippingPlane.add(new Edge(b, c));
				clippingPlane.add(new Edge(c, d));
				clippingPlane.add(new Edge(d, a));

				clippingPlane.add(new Edge(e, f));
				clippingPlane.add(new Edge(f, g));
				clippingPlane.add(new Edge(g, h));
				clippingPlane.add(new Edge(h, e));

				clippingPlane.add(new Edge(a, e));
				clippingPlane.add(new Edge(b, f));
				clippingPlane.add(new Edge(c, g));
				clippingPlane.add(new Edge(d, h));
				
//				Point plusX = new Point(1, 0, 0);
//				Point minusX = new Point(-1, 0, 0);
//				Point plusY = new Point(0, 1, 0);
//				Point minusY = new Point(0, -1, 0);
//				Point plusZ = new Point(0, 0, 1);
//				Point minusZ = new Point(0, 0, -1);
//				
//				clippingPlane.add(new Edge(new Point(bounds[0] + bounds[3], 0, 0), plusX));
//				clippingPlane.add(new Edge(new Point(bounds[0], 0, 0), minusX));
//				clippingPlane.add(new Edge(new Point(0, bounds[1] + bounds[4], 0), plusY));
//				clippingPlane.add(new Edge(new Point(0, bounds[1], 0), minusY));
//				clippingPlane.add(new Edge(new Point(0, 0, bounds[2] + bounds[5]), plusZ));
//				clippingPlane.add(new Edge(new Point(0, 0, bounds[2]), minusZ));

//				clippingPlane.add(new Edge(a, c));
//				clippingPlane.add(new Edge(b, g));
//				clippingPlane.add(new Edge(f, h));
//				clippingPlane.add(new Edge(a, h));
//				clippingPlane.add(new Edge(d, g));
//				clippingPlane.add(new Edge(a, f));

				polygonClipping = new PolygonClipping();
			}
			if (quantizeVertices) {
				ObjectNode vqmNode = (ObjectNode) geometrySettings.get("vertexQuantizationMatrices");
				Iterator<String> fieldNames = vqmNode.fieldNames();
				vertexQuantizationMatrices = new HashMap<>();
				while (fieldNames.hasNext()) {
					String key = fieldNames.next();
					long roid = Long.parseLong(key);
					float[] vertexQuantizationMatrix = new float[16];
					ArrayNode mNode = (ArrayNode) vqmNode.get(key);
					vertexQuantizationMatrices.put(roid, vertexQuantizationMatrix);
					int i=0;
					for (JsonNode v : mNode) {
						vertexQuantizationMatrix[i++] = v.floatValue();
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
		
//		modelBounds = new Bounds(minBounds, maxBounds);
//		modelBoundsUntranslated = new Bounds(projectInfo.getBoundsUntranslated());
	}

	private boolean writeData() throws IOException, SerializerException {
		if (next == null) {
			return false;
		}
		if (GeometryPackage.eINSTANCE.getGeometryInfo() == next.eClass()) {
			writeGeometryInfo(next);
		} else if (GeometryPackage.eINSTANCE.getGeometryData() == next.eClass()) {
			writeGeometryData(next, next.getOid());
		} else {
			try {
				next = objectProvider.next();
			} catch (BimserverDatabaseException e) {
				e.printStackTrace();
			}
			return writeData();
		}
		try {
			next = objectProvider.next();
		} catch (BimserverDatabaseException e) {
			e.printStackTrace();
		}
		return next != null;
	}

	private void writeGeometryInfo(HashMapVirtualObject info) throws IOException, SerializerException {
		EStructuralFeature hasTransparencyFeature = info.eClass().getEStructuralFeature("hasTransparency");
		byte[] transformation = (byte[]) info.eGet(info.eClass().getEStructuralFeature("transformation"));
		long dataOid = (long) info.eGet(info.eClass().getEStructuralFeature("data"));
		
		serializerDataOutputStream.writeByte(MessageType.GEOMETRY_INFO.getId());
		long oid = (long) info.eGet(GeometryPackage.eINSTANCE.getGeometryInfo_IfcProductOid());
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
		
		if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
			minX = minX * projectInfo.getMultiplierToMm();
			minY = minY * projectInfo.getMultiplierToMm();
			minZ = minZ * projectInfo.getMultiplierToMm();
			maxX = maxX * projectInfo.getMultiplierToMm();
			maxY = maxY * projectInfo.getMultiplierToMm();
			maxZ = maxZ * projectInfo.getMultiplierToMm();
		}
		
		serializerDataOutputStream.ensureExtraCapacity(8 * 6 + ((byte[])transformation).length + 8);
		serializerDataOutputStream.writeDoubleUnchecked(minX);
		serializerDataOutputStream.writeDoubleUnchecked(minY);
		serializerDataOutputStream.writeDoubleUnchecked(minZ);
		serializerDataOutputStream.writeDoubleUnchecked(maxX);
		serializerDataOutputStream.writeDoubleUnchecked(maxY);
		serializerDataOutputStream.writeDoubleUnchecked(maxZ);
		lastTransformation = ByteBuffer.wrap(transformation);
		lastTransformation.order(ByteOrder.LITTLE_ENDIAN);
		asDoubleBuffer = lastTransformation.asDoubleBuffer();
		ms = new double[16];
		for (int i=0; i<16; i++) {
			ms[i] = asDoubleBuffer.get();
		}
//		if (!Matrix.invertM(inverse , 0, ms, 0)) {
//			System.out.println("NO INVERSE!");
//		}
		
		serializerDataOutputStream.write(transformation);
		
		long newDataOid = -1;
		if (dataOidsWritten.containsKey(dataOid)) {
			// Make up a unique id...
			newDataOid = dataOid + 10000000000L + dataOidsWritten.get(dataOid);
			serializerDataOutputStream.writeLong(newDataOid);
		} else {
			serializerDataOutputStream.writeLong(dataOid);
		}
		
		nrObjectsWritten++;
		if (reportProgress) {
			if (progressReporter != null) {
				progressReporter.update(nrObjectsWritten, size);
			}
		}
		
		if (dataOidsWritten.containsKey(dataOid)) {
			HashMapVirtualObject data = objectProvider.getByOid(dataOid);
			writeGeometryData(data, newDataOid);
			// This data was written before (and splitTriangles is on)
			// We need to write the data again (deduplicate) because the clipping should probably be different
		}
	}

	private void writeGeometryData(HashMapVirtualObject data, long oid) throws IOException, SerializerException {
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
				
				long roid = data.getRoid();
				serializerDataOutputStream.writeLong(roid);
				serializerDataOutputStream.writeLong((boolean)data.eGet(hasTransparencyFeature) ? 1 : 0);
				serializerDataOutputStream.writeLong(data.getOid());
				
				ByteBuffer indicesBuffer = ByteBuffer.wrap(indices);
				indicesBuffer.order(ByteOrder.LITTLE_ENDIAN);
				
				serializerDataOutputStream.writeInt(indicesBuffer.capacity() / 4);
				if (useSmallInts) {
					IntBuffer intBuffer = indicesBuffer.asIntBuffer();
					serializerDataOutputStream.ensureExtraCapacity(intBuffer.capacity() * 2);
					for (int i=0; i<intBuffer.capacity(); i++) {
						serializerDataOutputStream.writeShortUnchecked((short) intBuffer.get());
					}
				} else {
					serializerDataOutputStream.write(indicesBuffer.array());
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
			int reused = (int) data.get("reused");
			serializerDataOutputStream.writeInt(reused);

			short cid = (short) data.get("type");
			String type = objectProvider.getEClassForCid(cid).getName();
			serializerDataOutputStream.writeUTF(type);
			serializerDataOutputStream.align8();
			long roid = data.getRoid();
			serializerDataOutputStream.writeLong(roid);

			serializerDataOutputStream.writeLong((boolean)data.eGet(hasTransparencyFeature) ? 1 : 0);
			serializerDataOutputStream.writeLong(oid);
			
			if (splitTriangles) {
				if (useSmallInts || quantizeVertices || quantizeNormals) {
					throw new UnsupportedOperationException();
				}
				
				if (dataOidsWritten.containsKey(data.getOid())) {
					dataOidsWritten.put(data.getOid(), dataOidsWritten.get(data.getOid()) + 1);
				} else {
					dataOidsWritten.put(data.getOid(), 1);
					objectProvider.cache(data);
				}
				
				ByteBuffer indicesBuffer = ByteBuffer.wrap(indices);
				indicesBuffer.order(ByteOrder.LITTLE_ENDIAN);
				IntBuffer indicesInt = indicesBuffer.asIntBuffer();
				
				ByteBuffer vertexByteBuffer = ByteBuffer.wrap(vertices);
				vertexByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
				FloatBuffer vertexBuffer = vertexByteBuffer.asFloatBuffer();

				ByteBuffer normalsByteBuffer = ByteBuffer.wrap(normals);
				normalsByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
				FloatBuffer normalBuffer = normalsByteBuffer.asFloatBuffer();
				
				ByteBuffer materialsByteBuffer = ByteBuffer.wrap(materials);
				materialsByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
				FloatBuffer materialsFloatBuffer = materialsByteBuffer.asFloatBuffer();
				
				List<Integer> newIndices = new ArrayList<>();
				List<Float> newVertices = new ArrayList<>();
				List<Float> newColors = new ArrayList<>();
				List<Float> newNormals = new ArrayList<>();

				int c = 0;
				for (int i=0; i<indicesInt.capacity(); i+=3) {
					int index1 = indicesInt.get(i);
					int index2 = indicesInt.get(i + 1);
					int index3 = indicesInt.get(i + 2);
					
					// Assuming all 3 vertices have the same normal and that it stays the same...
					float[] normal = new float[]{normalBuffer.get(index1 * 3), normalBuffer.get(index1 * 3 + 1), normalBuffer.get(index1 * 3 + 2)};
					float[] triangleColor = null;
					if (!useSingleColors && materials != null && color == null) {
						triangleColor = new float[]{materialsFloatBuffer.get(index1 * 3), materialsFloatBuffer.get(index1 * 3 + 1), materialsFloatBuffer.get(index1 * 3 + 2), materialsFloatBuffer.get(index1 * 3 + 3)};
					}
					
					float[] r = new float[4];
					
//					C = (P1+P2+P3)/3 = ((x1+x2+x3)/3,(y1+y2+y3)/3,(z1+z2+z3)/3)
					
					ArrayList<Point> points = new ArrayList<>();

					float[] v1 = new float[]{vertexBuffer.get(index1 * 3), vertexBuffer.get(index1 * 3 + 1), vertexBuffer.get(index1 * 3 + 2), 1};
					Matrix.multiplyMV(r, 0, ms, 0, v1, 0);
					v1 = new float[]{r[0], r[1], r[2], r[3]};
					if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
						v1[0] = v1[0] * projectInfo.getMultiplierToMm();
						v1[1] = v1[1] * projectInfo.getMultiplierToMm();
						v1[2] = v1[2] * projectInfo.getMultiplierToMm();
					}
					points.add(new Point(v1[0], v1[1], v1[2]));

					float[] v2 = new float[]{vertexBuffer.get(index2 * 3), vertexBuffer.get(index2 * 3 + 1), vertexBuffer.get(index2 * 3 + 2), 1};
					Matrix.multiplyMV(r, 0, ms, 0, v2, 0);
					v2 = new float[]{r[0], r[1], r[2], r[3]};
					if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
						v2[0] = v2[0] * projectInfo.getMultiplierToMm();
						v2[1] = v2[1] * projectInfo.getMultiplierToMm();
						v2[2] = v2[2] * projectInfo.getMultiplierToMm();
					}
					points.add(new Point(v2[0], v2[1], v2[2]));

					float[] v3 = new float[]{vertexBuffer.get(index3 * 3), vertexBuffer.get(index3 * 3 + 1), vertexBuffer.get(index3 * 3 + 2), 1};
					Matrix.multiplyMV(r, 0, ms, 0, v3, 0);
					v3 = new float[]{r[0], r[1], r[2], r[3]};
					if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
						v3[0] = v3[0] * projectInfo.getMultiplierToMm();
						v3[1] = v3[1] * projectInfo.getMultiplierToMm();
						v3[2] = v3[2] * projectInfo.getMultiplierToMm();
					}
					points.add(new Point(v3[0], v3[1], v3[2]));
					
					float[] centroid = new float[]{(v1[0] + v2[0] + v3[0])/3f, (v1[1] + v2[1] + v3[1])/3f, (v1[2] + v2[2] + v3[2])/3f};
					if (centroid[0] >= bounds[0] && centroid[1] >= bounds[1] && centroid[2] >= bounds[2] && centroid[0] <= bounds[0] + bounds[3] && centroid[1] <= bounds[1] + bounds[4] && centroid[2] <= bounds[2] + bounds[5]) {
						newVertices.add(v1[0]);
						newVertices.add(v1[1]);
						newVertices.add(v1[2]);
						
						newVertices.add(v2[0]);
						newVertices.add(v2[1]);
						newVertices.add(v2[2]);

						newVertices.add(v3[0]);
						newVertices.add(v3[1]);
						newVertices.add(v3[2]);
						
						if (!useSingleColors && materials != null && color == null) {
							newColors.add(triangleColor[0]);
							newColors.add(triangleColor[1]);
							newColors.add(triangleColor[2]);
							newColors.add(triangleColor[3]);
							newColors.add(triangleColor[0]);
							newColors.add(triangleColor[1]);
							newColors.add(triangleColor[2]);
							newColors.add(triangleColor[3]);
							newColors.add(triangleColor[0]);
							newColors.add(triangleColor[1]);
							newColors.add(triangleColor[2]);
							newColors.add(triangleColor[3]);
						}
						
						newNormals.add(normal[0]);
						newNormals.add(normal[1]);
						newNormals.add(normal[2]);
						
						newNormals.add(normal[0]);
						newNormals.add(normal[1]);
						newNormals.add(normal[2]);
						
						newNormals.add(normal[0]);
						newNormals.add(normal[1]);
						newNormals.add(normal[2]);
						
						newIndices.add(c + 0);
						newIndices.add(c + 1);
						newIndices.add(c + 2);
						
						c += 3;
					}
					
//					try {
//						ArrayList<Point> results = polygonClipping.clippingAlg(points, clippingPlane);
//						for (int j=0; j<results.size(); j++) {
//							Point a = results.get(j);
//							
//							newVertices.add((float) a.getX());
//							newVertices.add((float) a.getY());
//							newVertices.add((float) a.getZ());
//							
//							newNormals.add(normal[0]);
//							newNormals.add(normal[1]);
//							newNormals.add(normal[2]);
//						}
//						
//						if (results.size() > 0) {
//							for (int j=1; j<results.size() - 1; j++) {
//								newIndices.add(c + 0); // First vertex, making a fan
//								newIndices.add(c + j);
//								newIndices.add(c + j + 1);
//							}
//						}
//						
//						c += results.size();
//					} catch (ArrayIndexOutOfBoundsException e) {
//						e.printStackTrace();
//					}
				}
				
				serializerDataOutputStream.writeInt(newIndices.size());
				for (int index : newIndices) {
					serializerDataOutputStream.writeInt(index);
				}
				
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
				serializerDataOutputStream.writeInt(newVertices.size());
				for (int i=0; i<newVertices.size(); i+=3) {
					float[] v = new float[]{newVertices.get(i), newVertices.get(i + 1), newVertices.get(i + 2), 1};
					float[] r = new float[4];
					if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
						v[0] = v[0] / projectInfo.getMultiplierToMm();
						v[1] = v[1] / projectInfo.getMultiplierToMm();
						v[2] = v[2] / projectInfo.getMultiplierToMm();
					}
					// TODO !!!! When the inverse is not valid, we need to do something with this information
					Matrix.multiplyMV(r, 0, inverse, 0, v, 0);
					if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
						r[0] = r[0] * projectInfo.getMultiplierToMm();
						r[1] = r[1] * projectInfo.getMultiplierToMm();
						r[2] = r[2] * projectInfo.getMultiplierToMm();
					}
					serializerDataOutputStream.writeFloat(r[0]);
					serializerDataOutputStream.writeFloat(r[1]);
					serializerDataOutputStream.writeFloat(r[2]);
				}
				
				serializerDataOutputStream.writeInt(newNormals.size());
				for (float v : newNormals) {
					serializerDataOutputStream.writeFloat(v);
				}

				// Only when materials are used we send them
				if (!useSingleColors && materials != null && color == null) {
					serializerDataOutputStream.writeInt(newColors.size());
					for (int i=0; i<newColors.size(); i++) {
						serializerDataOutputStream.writeFloat(newColors.get(i));
					}
				} else {
					// No materials used
					serializerDataOutputStream.writeInt(0);
				}					
			} else {
				ByteBuffer indicesBuffer = ByteBuffer.wrap(indices);
				indicesBuffer.order(ByteOrder.LITTLE_ENDIAN);
				serializerDataOutputStream.writeInt(indicesBuffer.capacity() / 4);
				if (useSmallInts) {
					IntBuffer intBuffer = indicesBuffer.asIntBuffer();
					serializerDataOutputStream.ensureExtraCapacity(intBuffer.capacity() * 2);
					for (int i=0; i<intBuffer.capacity(); i++) {
						serializerDataOutputStream.writeShortUnchecked((short) intBuffer.get());
					}
				} else {
					serializerDataOutputStream.write(indicesBuffer.array());
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
				int nrPos = vertexByteBuffer.capacity() / 4;
				serializerDataOutputStream.writeInt(nrPos);
				if (quantizeVertices) {
					vertexByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
					serializerDataOutputStream.ensureExtraCapacity(vertexByteBuffer.capacity() * 6 / 4);
					float[] vertex = new float[4];
					float[] result = new float[4];
					vertex[3] = 1;
					for (int i=0; i<nrPos; i+=3) {
						vertex[0] = vertexByteBuffer.getFloat();
						vertex[1] = vertexByteBuffer.getFloat();
						vertex[2] = vertexByteBuffer.getFloat();
						
						if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
							vertex[0] = vertex[0] * projectInfo.getMultiplierToMm();
							vertex[1] = vertex[1] * projectInfo.getMultiplierToMm();
							vertex[2] = vertex[2] * projectInfo.getMultiplierToMm();
						}
						
						float[] matrix = vertexQuantizationMatrices.get(roid);
						if (matrix == null) {
							LOGGER.error("Missing quant matrix for " + roid);
							return;
						}
						Matrix.multiplyMV(result, 0, matrix, 0, vertex, 0);
						
						serializerDataOutputStream.writeShortUnchecked((short)result[0]);
						serializerDataOutputStream.writeShortUnchecked((short)result[1]);
						serializerDataOutputStream.writeShortUnchecked((short)result[2]);
					}
					serializerDataOutputStream.align8();
				} else {
					if (normalizeUnitsToMM && projectInfo.getMultiplierToMm() != 1f) {
						vertexByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
						float[] vertex = new float[3];
						for (int i=0; i<nrPos; i+=3) {
							vertex[0] = vertexByteBuffer.getFloat();
							vertex[1] = vertexByteBuffer.getFloat();
							vertex[2] = vertexByteBuffer.getFloat();
							vertex[0] = vertex[0] * projectInfo.getMultiplierToMm();
							vertex[1] = vertex[1] * projectInfo.getMultiplierToMm();
							vertex[2] = vertex[2] * projectInfo.getMultiplierToMm();
							
							// TODO slow
							serializerDataOutputStream.writeFloat(vertex[0]);
							serializerDataOutputStream.writeFloat(vertex[1]);
							serializerDataOutputStream.writeFloat(vertex[2]);
						}
					} else {
						serializerDataOutputStream.write(vertexByteBuffer.array());
					}
				}
				
				ByteBuffer normalsBuffer = ByteBuffer.wrap(normals);
				serializerDataOutputStream.writeInt(normalsBuffer.capacity() / 4);
				if (quantizeNormals) {
					normalsBuffer.order(ByteOrder.LITTLE_ENDIAN);
					for (int i=0; i<normalsBuffer.capacity() / 4; i++) {
						float normal = normalsBuffer.getFloat();
						serializerDataOutputStream.writeByteUnchecked((int) (normal * 127));
					}
					serializerDataOutputStream.align8();
				} else {
					serializerDataOutputStream.write(normalsBuffer.array());
				}
				
				if (useSingleColors) {
					serializerDataOutputStream.writeInt(0);
				} else {
					if (materials == null) {
						// We need to generate them
						if (color == null) {
							serializerDataOutputStream.writeInt(0);
							return;
						}
						int nrVertices = vertices.length / 12;
						serializerDataOutputStream.writeInt(nrVertices * 4);
						serializerDataOutputStream.ensureExtraCapacity(nrVertices * 4 * (quantizeColors ? 1 : 4));
						byte[] quantizedColor = new byte[4];
						quantizedColor[0] = UnsignedBytes.checkedCast((long)((float)color.eGet("x") * 255f));
						quantizedColor[1] = UnsignedBytes.checkedCast((long)((float)color.eGet("y") * 255f));
						quantizedColor[2] = UnsignedBytes.checkedCast((long)((float)color.eGet("z") * 255f));
						quantizedColor[3] = UnsignedBytes.checkedCast((long)((float)color.eGet("w") * 255f));
						for (int i=0; i<nrVertices; i++) {
							if (quantizeColors) {
								serializerDataOutputStream.writeByte(quantizedColor[0]);
								serializerDataOutputStream.writeByte(quantizedColor[1]);
								serializerDataOutputStream.writeByte(quantizedColor[2]);
								serializerDataOutputStream.writeByte(quantizedColor[3]);
							} else {
								serializerDataOutputStream.writeFloatUnchecked((float) color.eGet("x"));
								serializerDataOutputStream.writeFloatUnchecked((float) color.eGet("y"));
								serializerDataOutputStream.writeFloatUnchecked((float) color.eGet("z"));
								serializerDataOutputStream.writeFloatUnchecked((float) color.eGet("w"));
							}
						}
					} else {
						ByteBuffer materialsByteBuffer = ByteBuffer.wrap(materials);
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
		}
	}

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