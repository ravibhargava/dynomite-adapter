package com.thomsonreuters.adapter.impl;

import java.io.Serializable;

import com.thomsonreuters.dynomite.client.DynomiteClient;

public interface SerializableWrapper extends Serializable {
	public DynomiteClient getClient();
}
