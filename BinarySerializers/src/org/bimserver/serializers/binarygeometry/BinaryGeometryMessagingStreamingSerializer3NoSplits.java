package org.bimserver.serializers.binarygeometry;

import org.bimserver.emf.PackageMetaData;
import org.bimserver.plugins.PluginManagerInterface;
import org.bimserver.plugins.serializers.ObjectProvider;
import org.bimserver.plugins.serializers.ProjectInfo;
import org.bimserver.plugins.serializers.SerializerException;

public class BinaryGeometryMessagingStreamingSerializer3NoSplits extends BinaryGeometryMessagingStreamingSerializer3 {
	@Override
	public void init(ObjectProvider objectProvider, ProjectInfo projectInfo, PluginManagerInterface pluginManager, PackageMetaData packageMetaData) throws SerializerException {
		super.init(objectProvider, projectInfo, pluginManager, packageMetaData);
		setSplitGeometry(false);
	}
}