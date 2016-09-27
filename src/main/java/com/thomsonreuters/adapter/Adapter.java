package com.thomsonreuters.adapter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;

public interface Adapter {
	
	public JavaRDD<String> fromDynomiteKey(String key);
	
	public JavaRDD<String> fromDynomiteKey(String[] key);
	
	public JavaPairRDD<String, String> fromDynomiteKV(String key);
	
	public JavaPairRDD<String, String> fromDynomiteKV(String[] key);

	public JavaRDD<String> fromDynomiteHash(String key);
	
	public JavaRDD<String> fromDynomiteHash(String[] key);
	
	public JavaRDD<String> fromDynomiteList(final String key);
	
	public JavaRDD<String> fromDynomiteList(String key[]);
	
	public JavaRDD<String> fromDynomiteSet(String key);
	
	public JavaRDD<String> fromDynomiteSet(String key[]);
	
	public DataFrame fromDynomiteDataFrame(final String key) throws Exception;

	public void toDynomiteKV(JavaPairRDD<String, String> stringRDD);
	
	public void toDynomiteHASH(JavaRDD<String> hashRDD, String hashName);
	
	public void toDynomiteLIST(JavaRDD<String> listRDD, String listName);
	
	public void toDynomiteSET(JavaRDD<String> listRDD, String listName);
	
	public void addlist(final String key, JavaRDD<String> list);
	
	public void toDynomiteDataFrame(final String key, DataFrame dataframe) throws Exception;
}