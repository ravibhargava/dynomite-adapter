package com.thomsonreuters.adapter.test;

import java.io.IOException;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Address extends UserDefinedType{
	private String address;
	private boolean primitive;
	private String zip;
	
	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public boolean isPrimitive() {
		return primitive;
	}

	public void setPrimitive(boolean primitive) {
		this.primitive = primitive;
	}

	public Address() {
		super();
	}
	
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	@Override
	public Object deserialize(Object json) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			Address address = mapper.readValue((String) json, Address.class );
			return (Object) address;
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Object serialize(Object arg0) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			String json = mapper.writeValueAsString(this);
			return json;
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public DataType sqlType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Class userClass() {
		// TODO Auto-generated method stub
		return null;
	}
}