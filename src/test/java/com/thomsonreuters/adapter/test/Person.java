package com.thomsonreuters.adapter.test;

import java.io.IOException;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.UserDefinedType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Person extends UserDefinedType{
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public Address getAddress() {
		return address;
	}
	public void setAddress(Address address) {
		this.address = address;
	}
	private String name;
	private int age;
	private Address address;
	private String json;
	public Person() {
		super();
	}
	@Override
	public Object deserialize(Object json) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			Person person = mapper.readValue( (String) json, Person.class );
			return person;
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
		    json = mapper.writeValueAsString(this);
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
