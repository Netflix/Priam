package com.netflix.priam.utils;

import org.apache.cassandra.tools.NodeProbe;

/*
 * Represents an entity interested in a change of state to the NodeTool
 */
public interface INodeToolObserver {

	public void nodeToolHasChanged(NodeProbe nodeTool);
}
