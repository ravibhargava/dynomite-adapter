package com.thomsonreuters.adapter.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.thomsonreuters.adapter.Adapter;
import com.thomsonreuters.dynomite.client.DynomiteClient;
import com.thomsonreuters.dynomite.client.DynomiteClientFactory;
import com.thomsonreuters.dynomite.client.types.sync.DynomiteList;
import com.thomsonreuters.dynomite.client.types.sync.DynomiteMap;
import com.thomsonreuters.dynomite.client.types.sync.DynomiteSet;

public class AdapterImpl implements Serializable, Adapter{

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
	
	public JavaRDD<String> fromDynomiteKey(String key) {
		String value = client.getClient().get(key);
		List<String> list = new ArrayList<String>();
		list.add(value);
		JavaRDD<String> rdd = sc.parallelize(list);
		return rdd;
	}
	
	public JavaRDD<String> fromDynomiteKey(String[] key) {
		return null;
	}
	public JavaPairRDD<String, Map<String, String>> fromDynomiteKV(String key) {
		String value = client.getClient().get(key);
		List<Tuple2<String, Map<String,String>>> tuple = new ArrayList<Tuple2<String, Map<String,String>>>();      
		Map<String, String> map = new HashMap<String, String>(); 
		map.put(key, value);
		tuple.add(new Tuple2(key,map));
		JavaPairRDD<String, Map<String,String>> rddpair = sc.parallelizePairs(tuple);
		return rddpair;
	}
	
	public JavaPairRDD<String, String> fromDynomiteKV(String[] key) {
		
		return null;
	}

	public JavaPairRDD<String, String> fromDynomiteHash(String key) {
		DynomiteMap value = client.getClient().hash(key);
		List<String> list = new ArrayList<String>();
		return null;
	}
	
	public JavaPairRDD<String, String> fromDynomiteHash(String[] key) {
		return null;
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
	
	public JavaRDD<String> fromDynomiteList(String key[]) {
		return null;
	}
	
	public JavaRDD<String> fromDynomiteSet(String key) {
		DynomiteSet dlist = client.getClient().set(key);
		Set<String> jlist = new HashSet<String>();
		return null;
	}
	
	public JavaRDD<String> fromDynomiteSet(String key[]) {
		return null;
	}
	
	@Override
	public DataFrame fromDynomiteDataFrame(String key) throws Exception {
		DataFrameMetadata dataFrameMetadata = DataFrameMetadata.getMetadata(key);
		return null;
	}

	public void toDynomiteKV(JavaPairRDD<String, String> stringRDD) {
		stringRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>(){
			public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
				while (iterator.hasNext()){
					Tuple2<String, String> s = iterator.next();
					client.getClient().put(s._1, s._2);
				}
		    }});
	}
	
	public void toDynomiteHASH(JavaPairRDD<String, String> hashRDD, final String hashName) {
		hashRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>(){
			public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
				while (iterator.hasNext()){
					Tuple2<String, String> s = iterator.next();
					DynomiteMap map = client.getClient().hash(hashName);
					map.put(s._1, s._2);
				}
		    }});		
		
	}
	
	public void toDynomiteLIST(JavaRDD<String> listRDD, final String listName) {
		listRDD.foreachPartition(new VoidFunction<Iterator<String>>(){
		public void call(Iterator<String> iterator) throws Exception {
			while (iterator.hasNext()){
				String s = iterator.next();
				client.getClient().list(listName).add(s);
			}
	    }});
			
	}
	
	public void toDynomiteSET(JavaRDD<String> setRDD, final String setName) {
		setRDD.foreachPartition(new VoidFunction<Iterator<String>>(){
		public void call(Iterator<String> iterator) throws Exception {
			while (iterator.hasNext()){
				String s = iterator.next();
				client.getClient().list(setName).add(s);
			}
	    }});

	}
	
	public void addlist(final String key, JavaRDD<String> list){	
		list.foreachPartition(new VoidFunction<Iterator<String>>(){
		public void call(Iterator<String> iterator) throws Exception {
			while (iterator.hasNext()){
				String s = iterator.next();
				client.getClient().list(key).add(s);
			}
	    }});
	}
	

	
	public void toDynomiteDataFrame(DataFrame dataframe, final String key) throws Exception {
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
}
