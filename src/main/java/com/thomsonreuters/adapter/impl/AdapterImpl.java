package com.thomsonreuters.adapter.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;

import scala.Tuple2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thomsonreuters.adapter.Adapter;
import com.thomsonreuters.dynomite.client.DynomiteClient;
import com.thomsonreuters.dynomite.client.DynomiteClientFactory;
import com.thomsonreuters.dynomite.client.types.sync.DynomiteList;
import com.thomsonreuters.dynomite.client.types.sync.DynomiteMap;
import com.thomsonreuters.dynomite.client.types.sync.DynomiteSet;

public class AdapterImpl implements Serializable, Adapter{

	private SerializableWrapper client = null;
	private transient JavaSparkContext sc;
	private transient SQLContext sqlcontext;
	public AdapterImpl(final JavaSparkContext sc) {
		client = new SerializableWrapper() {
		    public DynomiteClient getClient() {
		        return DynomiteClientFactory.getClient();
		    }
		};
		this.sc = sc;
		this.sqlcontext = new SQLContext(this.sc);
		initHash();
	}
	
	private List<String> keyHelper(String key) {
		String value = client.getClient().get(key);
		List<String> list = new ArrayList<String>();
		list.add(value);
		return list;
	}
	
	
	public JavaRDD<String> fromDynomiteKey(String key) {
		List<String> list = keyHelper(key);
		JavaRDD<String> rdd = sc.parallelize(list);
		return rdd;
	}
	
	public JavaPairRDD<String, String> fromDynomiteKV(String key) {
		String value = client.getClient().get(key);
		List<Tuple2<String, String>> listR = Arrays.asList(
                new Tuple2<String, String>(key, value));
		JavaPairRDD<String,String> rdd = sc.parallelizePairs(listR);
		return rdd;
	}
	
	private Tuple2<String, Map<String,String>> hashHelper(String k) {
		DynomiteMap dmap = client.getClient().hash(k);
		Map<String, String> map = new HashMap<String, String>(); 
		Iterator<Map.Entry<String,String>> iter = dmap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, String> m =iter.next();
			String key2 = m.getKey();
			String val = m.getValue(); 
			map.put(key2,val);
		}
		Tuple2<String, Map<String,String>> t2 = new Tuple2<String, Map<String,String>>(k,map);
		return t2;

	}
	
	public JavaPairRDD<String, Map<String,String>> fromDynomiteHash(String key) {
		DynomiteMap dmap = client.getClient().hash(key);
		List<Tuple2<String, Map<String,String>>> tuple = new ArrayList<Tuple2<String, Map<String,String>>>();      
		Tuple2<String, Map<String,String>> h = hashHelper(key);
		tuple.add(h);
		JavaPairRDD<String, Map<String,String>> rddpair = sc.parallelizePairs(tuple);
		return rddpair;
	}
	
	private List<String> listHelper(String k) {
		DynomiteList dlist = client.getClient().list(k);
		List<String> jlist = new ArrayList<String>();
		for (int i=0; i<dlist.size();i++) {
			jlist.add(dlist.get(i));
		}
		return jlist;
	}
	
	public JavaRDD<String> fromDynomiteList(final String key){	
		List<String> jlist = listHelper(key);
		JavaRDD<String> rdd = sc.parallelize(jlist);
		return rdd;
	}
	
	private List<String> setHelper(String key) {
		DynomiteSet dset = client.getClient().set(key);
		List<String> jset = new ArrayList<String>();  
		while (dset.iterator().hasNext()) {
			jset.add(dset.iterator().next());
		}
		return jset;
	}
	
	public JavaRDD<String> fromDynomiteSet(String key) {
		List<String> jset = setHelper(key);
		JavaRDD<String> rddpair = sc.parallelize(jset);
		return rddpair;
	}
	
	
	public JavaRDD<List<String>> fromDynomiteList(String key[]) {
		List<List<String>> list2 = new ArrayList<List<String>>();
		for (String k:key) {
			List<String> jset = setHelper(k);
			list2.add(jset);
		}
		JavaRDD<List<String>> rddpair = sc.parallelize(list2);
		return rddpair;
	}
	
	public JavaPairRDD<String, Map<String,String>> fromDynomiteHash(String[] key) {
		List<Tuple2<String, Map<String,String>>> tuples = new ArrayList<Tuple2<String, Map<String,String>>>(); 
		for (String k:key) {
			Tuple2<String, Map<String,String>> h = hashHelper(k);
			tuples.add(h);
		}
		JavaPairRDD<String, Map<String,String>> rddpair = sc.parallelizePairs(tuples);
		return rddpair;

	}
	public JavaPairRDD<String, String> fromDynomiteKV(String[] key) {
		List<Tuple2<String, String>> listR = new ArrayList<Tuple2<String, String>>();
		for (String k:key) {
			String value = client.getClient().get(k);
			Tuple2<String, String> t = new Tuple2<String, String>(k, value);
			listR.add(t);
		}
		JavaPairRDD<String,String> rdd = sc.parallelizePairs(listR);
		return rdd;
	}
	
	public JavaRDD<String> fromDynomiteKey(String[] key) {
		List <String> listR = new ArrayList<String>();
		for (String k:key) {
			String value = client.getClient().get(k);
			listR.add(value);
		}
		JavaRDD<String> rdd = sc.parallelize(listR);
		return rdd;
	}
	private static Map<String, DataType> dataTypeHash = new HashMap<String, DataType>();
	
	private void initHash() {
		dataTypeHash.put("BinaryType", DataTypes.BinaryType);
		dataTypeHash.put("BooleanType", DataTypes.BooleanType);
		dataTypeHash.put("ByteType", DataTypes.ByteType);
		dataTypeHash.put("DateType", DataTypes.DateType);
		dataTypeHash.put("StringType", DataTypes.StringType);
		dataTypeHash.put("DoubleType", DataTypes.DoubleType);
		dataTypeHash.put("FloatType", DataTypes.FloatType);
		dataTypeHash.put("IntegerType", DataTypes.IntegerType);
		dataTypeHash.put("LongType", DataTypes.LongType);
		dataTypeHash.put("ShortType", DataTypes.ShortType);
		dataTypeHash.put("TimestampType", DataTypes.TimestampType);	
	}
		
	
	@Override
	public DataFrame fromDynomiteDataFrame(String key) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		String metadata = client.getClient().get(key+":metadata");
		DataFrameMetadata df = mapper.readValue(metadata, DataFrameMetadata.class);
		long num_rows = df.getNum_rows();
		List<String> cols = df.getColumns();
		List<String> types = df.getTypes();
		String value = null;
		DataType dataType = null;
		Row row = RowFactory.create();
		List<Row> table = new ArrayList<Row>();
		List<String> line = new ArrayList<String>();
		List<StructField> rows = new ArrayList<StructField>();
		for (int k=0 ;k <cols.size();k++) {
			dataType = dataTypeHash.get(types.get(k));
			rows.add(DataTypes.createStructField(cols.get(k), dataType, true));
		}
		for (long i=0;i<num_rows;i++) {
			int index = (int)i;
			for (int k=0; k<cols.size();k++) {
				String metaKey = key+":"+cols.get(k)+":"+index; 
				System.out.println("READ: metaKey="+metaKey+" types="+types.get(k));
				dataType = dataTypeHash.get(types.get(k));
				if (dataType == null) {
					String className = client.getClient().get(metaKey+":"+"class");
					String struct = client.getClient().get(metaKey+":"+"struct");
					String json = client.getClient().get(metaKey+":"+"type");
					Class clazz = Class.forName(struct);
					System.out.println("Struct = "+struct);
					if (clazz.isInstance(new StructType()));
						dataType = new StructType();
					metaKey=metaKey+":"+className;
				}
				//rows.add(DataTypes.createStructField(cols.get(k), dataType, true));
				value = client.getClient().get(metaKey);
				System.out.println("from metaKeyvalue ="+value);
				line.add(value);
			}
			row = RowFactory.create(line);
			table.add(row);
			line = new ArrayList<String>();
		}
		StructType schema = DataTypes.createStructType(rows);
		JavaRDD<Row> rdd = sc.parallelize(table);
		DataFrame dataframe = sqlcontext.createDataFrame(rdd, schema);				
		return dataframe;
	}

	public void toDynomiteKV(JavaPairRDD<String, String> stringRDD) {
		stringRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>(){
			public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
				while (iterator.hasNext()){
					Tuple2<String, String> s = iterator.next();
					client.getClient().put(s._1,s._2);
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

	
	public void toDynomiteDataFrame(DataFrame dataframe, final String key) throws Exception {
		StructType schema= dataframe.schema();
		final StructField[] fields = schema.fields();
		final List<String> names = new ArrayList<String>();
		final List<String> types = new ArrayList<String>();
		JavaRDD<Row> row = dataframe.javaRDD();
		
		row.foreachPartition(new VoidFunction<Iterator<Row>>(){
			public void call(Iterator<Row> iterator) throws Exception {
				long num_rows = 0;
				for (int i=0;i<fields.length;i++) {
					names.add(fields[i].name());
					types.add(fields[i].dataType().toString());	
				}
				while (iterator.hasNext()){
					Row row = iterator.next();
					for (int index=0;index<row.size();index++) {
						String name = fields[index].name();
						DataType type = fields[index].dataType();
						String metaKey= key+":"+name+":"+num_rows;
						System.out.println("WRITE:metaKey="+metaKey);
						if (type == DataTypes.BooleanType) {
							Boolean bool = row.getBoolean(index);
							System.out.println("value ="+bool);
							client.getClient().put(metaKey, new Boolean(bool).toString());
						}
						else if (type == DataTypes.ByteType) {
							Byte byteVal = row.getByte(index);
							System.out.println("value ="+byteVal);
							client.getClient().put(metaKey, byteVal.toString());
						}
						else if (type == DataTypes.DateType) {
							Date date = row.getDate(index);
							System.out.println("value ="+date);
							client.getClient().put(metaKey, date.toString());
						}
						else if (type == DataTypes.StringType) {
							String val = row.getString(index);
							System.out.println("value ="+val);
							client.getClient().put(metaKey, val);
						}		
						else if (type == DataTypes.DoubleType) {
							Double d = row.getDouble(index);
							System.out.println("value ="+d);
							client.getClient().put(metaKey, d.toString());
						}
						else if (type == DataTypes.FloatType) {
							Float f = row.getFloat(index);
							System.out.println("value ="+f);
							client.getClient().put(metaKey, f.toString());
						}
						else if (type == DataTypes.IntegerType) {
							Integer intVal = row.getInt(index);
							System.out.println("value ="+intVal);
							client.getClient().put(metaKey, intVal.toString());
						}
						else if (type == DataTypes.LongType) {
							Long longVal = row.getLong(index);
							System.out.println("value ="+longVal);
							client.getClient().put(metaKey, longVal.toString());
						}
						else if (type == DataTypes.ShortType) {
							Short shortVal = row.getShort(index);
							System.out.println("value ="+shortVal);
							client.getClient().put(metaKey, shortVal.toString());
						}
						else if (type == DataTypes.TimestampType) {
							String ts = row.getString(index);
							System.out.println("value ="+ts);
							client.getClient().put(metaKey, ts);
						}
						else if (type == DataTypes.NullType) {
							Boolean ts = row.isNullAt(index);
							System.out.println("value ="+ts);
							client.getClient().put(metaKey, ts.toString());
						}
						else {
							String struct = type.getClass().getName();
							UserDefinedType<?> udf = (UserDefinedType<?>)row.get(index);
							String json = (String) udf.serialize(udf);
							String clazz = udf.getClass().getName();
							client.getClient().put(metaKey+":"+"class", clazz);
							client.getClient().put(metaKey+":"+clazz, json);
							client.getClient().put(metaKey+":"+"struct", struct);
							client.getClient().put(metaKey+":"+"type", type.json());
						}
					}
					num_rows++;
				}
				DataFrameMetadata metadata = new DataFrameMetadata(client, key, names, types, num_rows);
				metadata.save();	
			}});
	}
}

//else if (type == DataTypes.BinaryType) {
//	//Array[Byte]  = row.(index);
//}
//def loop(path: String, dt: DataType, acc:Seq[String]): Seq[String] = {
//		  dt match {
//		  case s: ArrayType =>
//		       loop(path, s.elementType, acc)
//		  case s: StructType =>      
//		    s.fields.flatMap(f => loop(path + "." + f.name, f.dataType, acc))
//		  case other => 
//		    acc:+ path
//		}
//
//case "StructType":
//Row row2 = row.getStruct(index);					
////client.getClient().put(metaKey, struct);
//break;

//	else if (type == DataTypes.MapType) {
//		Map<Object, Object> map = row.getJavaMap(index);
//		client.getClient().put(metaKey, map.toString());
//case "ArrayType":
//	break;
//case "BinaryType":
//	Array[Byte]  = row.(index);
//	client.getClient().put(metaKey, byteVal.toString());
//	break;
//case "UserDefinedType":
//	String val2 = row.getString(index);
//	client.getClient().put(metaKey, val2);
//	break;
//case "StructType":
//	Row row2 = row.getStruct(index);					
//	//client.getClient().put(metaKey, struct);
//	break;


//StructType schema = DataTypes
//.createStructType(new StructField[] {
//		DataTypes.createStructField("id", DataTypes.IntegerType, false),
//		DataTypes.createStructField("name", DataTypes.StringType, false),
//		DataTypes.createStructField("url", DataTypes.StringType, true),
//		DataTypes.createStructField("pictures", DataTypes.StringType, true),
//		DataTypes.createStructField("time", DataTypes.TimestampType, true) });
//public JavaRDD<String> fromDynomiteSet(String key[]) {
//	return null;
//}

