package com.thomsonreuters.adapter;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;

public interface Adapter {
	
	/**
	 * Returns the JavaRDD<String> by retrieving the names from Dynomite that match the 
	 * key.
	 * @param key
	 * @return JavaRDD<String>
	 */
	public JavaRDD<String> fromDynomiteKey(String key);
	
	/**
	 * Returns the JavaPairRDD<String, String> that contains the string values of all keys whose names 
	 * match key. 
	 * @param key
	 * @return JavaPairRDD<String, String>
	 */
	
	public JavaPairRDD<String, Map<String, String>> fromDynomiteKV(String key);

	/**
	 * Returns the JavaPairRDD<String, String> with the fields and values of the hashName.
	 * @param key
	 * @return JavaPairRDD<String, String>
	 */
	
	public JavaPairRDD<String, Map<String,String>> fromDynomiteHash(String hashName); 
	
	/**
	 * Returns the contents (members) of the Dynomite Lists whose name is equal to listName.
	 * @param key
	 * @return JavaRDD<String>
	 */
	
	public JavaRDD<String> fromDynomiteList(final String listName);
	
	/**
	 * Returns the Dynomite Sets' members whose name is 'setName'.
	 * @param key
	 * @return JavaRDD<String>
	 */
	
	public JavaRDD<String> fromDynomiteSet(String setName);
	
	/**
	 * Returns the Dynomite Dataframe for the key.
	 * @param key
	 * @return DataFrame
	 */
	
	public DataFrame fromDynomiteDataFrame(final String key) throws Exception;
	
	/**
	 * Store the key-value pairs to Dynomite.
	 * @param stringRDD
	 */

	public void toDynomiteKV(JavaPairRDD<String, String> stringRDD);
	
	/**
	 * Store a hash (field-value pairs) with the key name specified by `hashName`.
	 * @param hashRDD
	 * @param hashName
	 */
	public void toDynomiteHASH(JavaPairRDD<String, String> hashRDD, String hashName);
	
	/**
	 * Store a list with the key name specified by `listName`.
	 * @param listRDD
	 * @param listName
	 */
	public void toDynomiteLIST(JavaRDD<String> listRDD, String listName);
	
	/**
	 * Store a set with the key name specified by `setName`.
	 * @param setRDD
	 * @param setName
	 */
	public void toDynomiteSET(JavaRDD<String> setRDD, String setName);
	
	/**
	 * Store a DataFrame with the key name specified by `dataName`.
	 * @param key
	 * @param dataframe
	 * @throws Exception
	 */
	public void toDynomiteDataFrame(DataFrame dataframe, final String dataName) throws Exception;
}

///**
// * Returns the JavaPairRDD<String, String> that contains the string values of all keys whose names 
// * match an array of keys.
// * @param key
// * @return JavaPairRDD<String, String>
// */
//
//public JavaPairRDD<String, String> fromDynomiteKV(String[] key);
//
///**
// * Returns the JavaPairRDD<String, String> with the fields and values of the hashes' names are 
// * provided in an array.
// * @param key
// * @return JavaPairRDD<String, String>
// */
//
////public JavaPairRDD<String, Map<String,String>> fromDynomiteHash(String[] key);
//
///**
// * Returns the contents (members) of the Dynomite Lists whose names are in the array.
// * @param key
// * @return JavaRDD<String>
// */
//
////public JavaRDD<String> fromDynomiteList(String key[]);
///**
// * Returns the Dynomite Sets' members whose names are in the array.
// * @param key
// * @return JavaRDD<String>
// */
////public JavaRDD<String> fromDynomiteSet(String key[]);
///**
// * Returns the JavaRDD<String> by retrieving the names from Dynomite that match the 
// * keys listed in an Array.
// * @param key[]
// * @return JavaRDD<String>
// */
//
////public JavaRDD<String> fromDynomiteKey(String[] key);