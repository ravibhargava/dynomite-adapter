package com.thomsonreuters.adapter.impl;

import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.types.DataType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class DataFrameMetadata {
	private long num_rows;
	private List<String> columns;
	private List<String> types;
	private String key;
	private SerializableWrapper client;
	public long getNum_rows() {
		return num_rows;
	}
	public void setNum_rows(long num_rows) {
		this.num_rows = num_rows;
	}
	public List<String> getColumns() {
		return columns;
	}
	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
	public List<String> getTypes() {
		return types;
	}
	public void setTypes(List<String> types) {
		this.types = types;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public DataFrameMetadata () {}
	
	public DataFrameMetadata (SerializableWrapper client, String key, List<String> columns, List<String> types, long num_rows) {
		this.client = client;
		this.key = key;
		this.columns = columns;
		this.types = types;
		this.num_rows = num_rows;
	}
	public void save() throws JsonProcessingException {
		String metadataKey = key+":metadata";
		ObjectMapper mapper = new ObjectMapper();
		String jsonString = mapper.writeValueAsString(this);
		client.getClient().put(metadataKey, jsonString);
	}
	
	public DataFrameMetadata getDataFrame(String key) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		String metadata = client.getClient().get(key+":metadata");
		DataFrameMetadata dataframe = mapper.readValue(metadata, DataFrameMetadata.class);
		return dataframe;
	}
}

