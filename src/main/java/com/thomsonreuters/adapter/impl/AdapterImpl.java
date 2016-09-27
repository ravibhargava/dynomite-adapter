package com.thomsonreuters.adapter.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.thomsonreuters.dynomite.client.DynomiteClient;
import com.thomsonreuters.dynomite.client.DynomiteClientFactory;
import com.thomsonreuters.dynomite.client.types.sync.DynomiteList;

public class AdapterImpl implements Serializable{

	private SerializableWrapper client = null;
	private transient JavaSparkContext sc;
	public AdapterImpl(final JavaSparkContext sc) {
		client = new SerializableWrapper() {
		    public DynomiteClient getClient() {
		        return DynomiteClientFactory.getClient();
		    }
		};
		this.sc = sc;
	}
	
	public void putString(final String key, String string){	
		client.getClient().put(key, string);
	}
	
	public JavaRDD<String> fromDynomiteList(final String key){	
		DynomiteList dlist = client.getClient().list(key);
		List<String> jlist = new ArrayList<String>();
		for (int i=0; i<dlist.size();i++) {
			jlist.add(dlist.get(i));
		}
		JavaRDD<String> rdd = sc.parallelize(jlist);
		return rdd;
	}
	
	public JavaRDD<String> fromDynomiteKey(String key) {
		return null;
	}
	
	public JavaRDD<String> fromDynomiteKey(String[] key) {
		return null;
	}
	public JavaPairRDD<String, String> fromDynomiteKV(String key) {
		return null;
	}
	
	public JavaPairRDD<String, String> fromDynomiteKV(String[] key) {
		return null;
	}

	public JavaRDD<String> fromDynomiteHash(String key) {
		return null;
	}
	
	public JavaRDD<String> fromDynomiteHash(String[] key) {
		return null;
	}
	
	public JavaRDD<String> fromDynomiteList(String key[]) {
		return null;
	}
	
	public JavaRDD<String> fromDynomiteSet(String key) {
		return null;
	}
	
	public JavaRDD<String> fromDynomiteSet(String key[]) {
		return null;
	}

	public void toDynomiteKV(JavaPairRDD<String, String> stringRDD) {}
	
	public void toDynomiteHASH(JavaRDD<String> hashRDD, String hashName) {}
	
	public void toDynomiteLIST(JavaRDD<String> listRDD, String listName) {}
	
	public void toDynomiteSET(JavaRDD<String> listRDD, String listName) {}
	
	public void addlist(final String key, JavaRDD<String> list){	
		list.foreachPartition(new VoidFunction<Iterator<String>>(){
		public void call(Iterator<String> iterator) throws Exception {
			while (iterator.hasNext()){
				String s = iterator.next();
				client.getClient().list(key).add(s);
			}
	    }});
	}
	

	
	public void toDynomiteDataFrame(final String key, DataFrame dataframe) throws Exception {
		StructType type= dataframe.schema();
		final StructField[] fields = type.fields();
		final List<String> columns = new ArrayList<String>();
		final List<String> types = new ArrayList<String>();
		JavaRDD<Row> row = dataframe.javaRDD();
		
		row.foreachPartition(new VoidFunction<Iterator<Row>>(){
			public void call(Iterator<Row> iterator) throws Exception {
				long num_rows = 0;
				for (int i=0;i<fields.length;i++) {
					columns.add(fields[i].name());
					types.add(fields[i].dataType().typeName());	
				}
				while (iterator.hasNext()){
					Row row = iterator.next();
					for (int index=0;index<row.size();index++) {
						String name = fields[index].name();
						String type = fields[index].dataType().typeName();
						String metaKey= key+":"+name+":"+index;
						switch (type) {
						case "string":
							String val = row.getString(index);
							client.getClient().put(metaKey, val);
							break;
						case "boolean":
							Boolean bool = row.getBoolean(index);
							client.getClient().put(metaKey, new Boolean(bool).toString());
							break;
						case "int":
							Integer intType = row.getInt(index);
							client.getClient().put(metaKey, intType.toString());
							break;
						}
					}
					num_rows++;
				}
				DataFrameMetadata metadata = new DataFrameMetadata(client, key, columns, types, num_rows);
				metadata.save();	
			}});
	}
	
	public DataFrame getDataframe(String key) throws JsonParseException, JsonMappingException, IOException {
		DataFrameMetadata dataFrameMetadata = DataFrameMetadata.getMetadata(key);
		return null;
		
	}

}