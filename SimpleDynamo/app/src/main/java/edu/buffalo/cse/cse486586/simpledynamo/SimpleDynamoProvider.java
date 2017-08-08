package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class SimpleDynamoProvider extends ContentProvider {

	public static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	public static final String MESSAGES="messages";
	public static final String KEY="key";
	public static final String VALUE="value";
	public static final String ACK="acknowledgement";
	public static final String FIRST_PORT = "5562";
	static final int SERVER_PORT = 10000;

	public static final String[] PORTS = {"5562", "5556", "5554", "5558", "5560"};

	public static final String INSERT="insert";
	public static final String QUERY="query";
	public static final String DELETE="delete";
	public static final String REP_INSERT="replicateInsert";
	public static final String REP_DELETE="replicateDelete";
	public static final String CLEAR_DATA="clearData";
	public static final String BREAK_LOCK="breakLock";
	public static final String QUERY_ALL ="queryAll";
	public static final String QUERY_SUCC ="querySuccessor";
	public static final String SET_GET_ALL_CURSOR="setGetALlCursor";
	public static final String GET_KEYS_PRED="getKeysPredecessor";
	public static final String GET_KEYS_SUCC="getKeysSuccessor";
	public static final String UPDATE_FROM_PRED="updateFromPredecessor";
	public static final String UPDATE_FROM_SUCC="updateFromSuccessor";
	public static final String BREAK_GET_ALL_LOCK="breakGetAllLock";

	public static final String FAILED_PORT="failedPort,0";
	public static final String FAILED_PORT_VALUE="failedPort";
	public static final String UPDATE="update";
	public static final String FIRST="first";
	public static final String SECOND="second";

	SharedPreferences sharedPreferences;
	String[] successors = new String[2];
	String[] predecessors = new String[2];
	String myPort;
	String nodeId;
	BlockingQueue<Pair> blockingQueue = new LinkedBlockingDeque<Pair>();
	BlockingQueue<String> blockingQueueGetAll = new LinkedBlockingDeque<String>();
	MatrixCursor getAllCursor = new MatrixCursor(new String[]{KEY, VALUE});

	public class Pair{
		String key;
		String value;
		int version;

		public Pair(String key, String value,int version){
			this.key=key;
			this.value=value;
			this.version=version;
		}
	}


	public  String findPort(String key){
		Log.d(TAG, "Entering findPort Method");
		try {
			String keyHash = genHash(key);
			String portId = "";
			for(String port: PORTS){
				portId = genHash(port);
				if(portId.compareTo(keyHash) > 0){
					Log.d(TAG, "FindPort Method:port returned" +port);
					return port;
				}
			}
		} catch (NoSuchAlgorithmException e) {
			Log.d(TAG, "NoSuchAlgorithmException in findPort Method");
		}
		Log.d(TAG, "findPort Method:port returned" +FIRST_PORT);
		return FIRST_PORT;
	}


	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.d(TAG, "Entering method overridden delete method");
		if(selection.equals("@")){
			Log.d(TAG, "overridden delete method when query is @ start");
			sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
			SharedPreferences.Editor editor = sharedPreferences.edit();
			editor.clear();
			editor.commit();
			Log.d(TAG, "overridden delete method when query is @ end");
		}else if(selection.equals("*")){
			Log.d(TAG, "overridden delete method when query is * start");
			deleteAll();
			Log.d(TAG, "overridden delete method when query is * end");
		}else {
			Log.d(TAG, "overridden delete method when query is any key: start");
			delete(selection);
			Log.d(TAG, "overridden delete method when query is any key : end");
		}
		Log.d(TAG, "Exiting method overridden delete method");
		return 0;
	}

	public  void deleteAll(){
		Log.d(TAG, "Entering custom deleteAll method");
		sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
		SharedPreferences.Editor editor = sharedPreferences.edit();
		editor.clear();
		editor.commit();
		for(String port: PORTS) {
			if (!port.equals(myPort)) {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
						new Request(port,CLEAR_DATA,null,null,null));
			}
		}
		Log.d(TAG, "Exiting custom deleteAll method");
	}

	public void clearData(){
		Log.d(TAG, "Entering clearData method in "+myPort);
		sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
		SharedPreferences.Editor editor = sharedPreferences.edit();
		editor.clear();
		editor.commit();
		Log.d(TAG, "Exiting clearData method in" +myPort);
	}

	public void delete(String key){
		Log.d(TAG, "Entering custom delete method ");
		String port = findPort(key);
		Log.d(TAG, "custom delete method value of port: " + port);
		if(port.compareTo(myPort) == 0){
			sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
			SharedPreferences.Editor editor = sharedPreferences.edit();
			editor.remove(key);
			editor.commit();
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(successors[0],REP_DELETE,key,null,null));
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(successors[1],REP_DELETE,key,null,null));
			Log.d("The deleted key is", key);
		}else{
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(port,DELETE,key,null,null));
		}
	}

	public void replicateDelete(String key){
		Log.d(TAG, "Entering replicateDelete method");
		sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
		SharedPreferences.Editor editor = sharedPreferences.edit();
		editor.remove(key);
		editor.commit();
		Log.d(TAG, "Exiting replicateDelete method");
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		Log.d(TAG, "Entering overridden insert method ");
		insert(values.getAsString(KEY),values.getAsString(VALUE));
		Log.d(TAG, "Exiting overridden insert method ");
		return uri;
	}

	public void insert(String key, String value){
		Log.d(TAG, "Entering custom insert method for key: "+key+"at port:"+myPort);
		String port = findPort(key);
		if(port.compareTo(myPort) == 0){
			Log.d(TAG, "custom insert method : port = myPort = " + port+"for key: "+key);
			sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
			SharedPreferences.Editor editor = sharedPreferences.edit();
			Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
			if(map.containsKey(key)){
				String val = sharedPreferences.getString(key, "");
				Log.d(TAG, "custom insert method: map contains key: "+key+" value: "+val);
				String[] valArray = val.split(",");
				int version = Integer.parseInt(valArray[1]);
				version=version+1;
				value=value+","+version;
			}else{
				value=value+","+1;
				Log.d(TAG, "custom insert method: map does not contain key: "+key+" value: "+value);
			}
			editor.putString(key,value);
			editor.commit();
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(successors[0],REP_INSERT,key,value,null));
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(successors[1],REP_INSERT,key,value,null));

			Log.v("The inserted key is", key +"was inserted in emulator" + myPort);
		}else{
			Log.d(TAG, "custom insert method : port is different than my port it is: " + port);
			Map<String,String> map = new HashMap<String, String>();
			int index = Arrays.asList(PORTS).indexOf(port);
			if(index < 3){
				map.put(FIRST,PORTS[index+1]);
				map.put(SECOND,PORTS[index+2]);
			}else if(index == 3){
				map.put(FIRST,PORTS[4]);
				map.put(SECOND,PORTS[0]);
			}else{
				map.put(FIRST,PORTS[0]);
				map.put(SECOND,PORTS[1]);
			}
			Log.d(TAG, "custom insert method : second insert called " + port+"for key: "+key);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(port,INSERT,key,value,map));
		}
	}

	public void replicateInsert(String key,String value){
		Log.d(TAG, "Entering replicateInsert method at port: "+myPort+"for key: "+key);
		sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
		SharedPreferences.Editor editor = sharedPreferences.edit();
		Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
		if(map.containsKey(key)){
			String previousValue = sharedPreferences.getString(key, "");
			Log.d(TAG, "ReplicateInsert method map contains key: "+key+" value: "+previousValue);
			String[] prevValArray = previousValue.split(",");
			int previousVersion = Integer.parseInt(prevValArray[1]);
			String[] currentValArray = value.split(",");
			int currentVersion = Integer.parseInt(currentValArray[1]);
			if(currentVersion==0){

				Log.d(TAG, "ReplicateInsert method current version 0");
				currentVersion=previousVersion+1;
				value=currentValArray[0]+","+currentVersion;
				editor.putString(key,value);
			}else if(previousVersion<=currentVersion){
				editor.putString(key,value);
			}
			Log.d(TAG, "ReplicateInsert method previous value:"+previousValue);
			Log.d(TAG, "ReplicateInsert method current value:"+value);
		}else{
			String[] currentValArray = value.split(",");
			int currentVersion = Integer.parseInt(currentValArray[1]);
			if(currentVersion==0){
				value=currentValArray[0]+","+1;
				editor.putString(key,value);
			}else{
				editor.putString(key,value);
			}
			Log.d(TAG, "ReplicateInsert method map does not contain: "+key+" value: "+value);
		}
		editor.commit();
		Log.d(TAG, "Exiting replicateInsert method");
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		Log.d(TAG, "Entering overridden query method at port: "+myPort);
		Log.d(TAG, "overridden query :recover check done");
		if (selection.equals("@")) {
			Log.d(TAG, "overridden query method when query is @ start");
			MatrixCursor cursor = new MatrixCursor(new String[]{KEY, VALUE});
			sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
			Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				String[] valArray = entry.getValue().split(",");
				String value=valArray[0];
				cursor.addRow(new String[]{entry.getKey(), value});
			}
			Log.d(TAG, "overridden query method when query is @ end");
			Log.v("query", selection);
			return cursor;
		}else if (selection.equals("*")) {
			Log.d(TAG, "overridden query method when query is * start");
			return getAll();
		}
		else
			return query(selection,myPort);
	}
	public Cursor getAll(){
		//try {
			Log.d(TAG, "entering getAll  method with emu ID "+myPort+" and start node" + myPort);
			sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
			Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
			if(map!=null) {
				for (Map.Entry<String, String> entry : map.entrySet()) {
					synchronized (getAllCursor) {
						String[] valArray = entry.getValue().split(",");
						String value=valArray[0];
						getAllCursor.addRow(new String[]{entry.getKey(), value});
					}
				}
			}
			Log.d(TAG, "getAll  method: getallcursor has been set for initial node by initial node");
			for(String port:PORTS){
				Log.d(TAG, "getAll  method: inside loop for port: "+port);
				if(!port.equals(myPort)){
					Log.d(TAG, "getAll  method: client task called for: "+port);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
							new Request(port, QUERY_ALL,null,myPort,null));
				}
			}
			Log.d(TAG, "getAll  method: put the get all lock");
			while (blockingQueueGetAll.size()<4){
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					Log.d(TAG, "getAll  method: InterruptedException in while loop");
				}
			}
			//blockingQueueGetAll.take();
			Log.d(TAG, "getAll  method: this is after the blockingQueueGetAll.take() thing");
		/*} catch (InterruptedException e) {
			Log.e(TAG, "InterruptedException in getAll method");
		}*/

		return getAllCursor;
	}

	public void queryAll(String initialPort){
		Log.d(TAG, "Entering queryAll  method at port: "+myPort);
		sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
		Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
				new Request(initialPort, SET_GET_ALL_CURSOR,null,null,map));
		Log.d(TAG, "Exiting queryAll  method");
	}

	public void fillGetAllCursor(Map<String,String> map){
		Log.d(TAG, "Entering fillGetAllCursor  method at port: "+myPort);
		try {
			Log.d(TAG, "Entering fillGetAllCursor method at port: "+myPort);
			if (map != null) {
				Log.d(TAG, "Entering fillGetAllCursor method map is not null");
				for (Map.Entry<String, String> entry : map.entrySet()) {
					synchronized (getAllCursor) {
						String[] valArray = entry.getValue().split(",");
						String value=valArray[0];
						getAllCursor.addRow(new String[]{entry.getKey(), value});
					}
				}
			}
			blockingQueueGetAll.put("dummy");
			Log.d(TAG, "Exiting fillGetAllCursor method");
		}catch (InterruptedException exc){
			Log.e(TAG, "InterruptedException in fillGetAllCursor method");
		}
	}

	public void breakGetAllLock(){
		Log.d(TAG, "Entering breakGetAllLock method at port: "+myPort);
		try {
			blockingQueueGetAll.put("dummy");
		} catch (InterruptedException e) {
			Log.e(TAG, "InterruptedException in breakGetAllLock method");
			e.printStackTrace();
		}
	}

	public Cursor query(String key,String initialPort){
		Log.d(TAG, "Entering custom query method for port:"+myPort+"for key: "+key);
		Log.d(TAG, "custom query :recover check done");
		String port = findPort(key);
		MatrixCursor cursor = new MatrixCursor(new String[]{KEY, VALUE});
		PriorityQueue<Pair> keyValues = new PriorityQueue<Pair>(1,new Comparator<Pair>(){
				public int compare(Pair p1,Pair p2){
					return p2.version-p1.version;
		}
		});
		if (port.compareTo(myPort) == 0) {
			sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
			if(myPort.equals(initialPort)) {
				//cursor.addRow(new String[]{key, sharedPreferences.getString(key, "")});
				String val = sharedPreferences.getString(key, "");
				String[] valArray = val.split(",");
				keyValues.add(new Pair(key,valArray[0],Integer.parseInt(valArray[1])));
				Log.v("query", key);
				Log.d(TAG, "custom query method: the key was found on the same node where request was made "+port);
			}else {
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
						new Request(initialPort, BREAK_LOCK, key, sharedPreferences.getString(key, ""), null));
				Log.d(TAG, "custom query method: this is after the client task for breaklock was called");
			}

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(successors[0],QUERY_SUCC, key, initialPort,null));
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(successors[1],QUERY_SUCC, key, initialPort,null));

		}else {
			Map<String,String> map = new HashMap<String, String>();
			int index = Arrays.asList(PORTS).indexOf(port);
			if(index < 3){
				map.put(FIRST,PORTS[index+1]);
				map.put(SECOND,PORTS[index+2]);
			}else if(index == 3){
				map.put(FIRST,PORTS[4]);
				map.put(SECOND,PORTS[0]);
			}else{
				map.put(FIRST,PORTS[0]);
				map.put(SECOND,PORTS[1]);
			}
			Log.d(TAG, "custom query method: this is when QUERY is called for another port");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(port,QUERY, key, initialPort,map));
		}

		try {
			if(myPort.equals(initialPort)) {
				Log.d(TAG, "custom query method: next stmt is blockingQueue.take() ");
				while(true){
					Pair pair = blockingQueue.take();
					if(pair.key.equals(key)){
						Log.d(TAG, "custom query method: blockingQueue.take()  is broken and cursor key:  "+key+" value: "+pair.value);
						keyValues.add(pair);
						// cursor.addRow(new String[]{key, pair.value});
						if(keyValues.size()==3)break;
					}else{
						blockingQueue.put(pair);
					}
				}
				/*Map<String,Integer> majorityMap = new HashMap<String, Integer>();
				for(Pair pair:keyValues){
					if(!pair.value.equals(FAILED_PORT_VALUE)) {
						if (majorityMap.containsKey(pair.value)) {
							majorityMap.put(pair.value, majorityMap.get(pair.value) + 1);
						} else {
							majorityMap.put(pair.value, 1);
						}
					}
				}
				int max=0;
				String value="";
				for(Map.Entry<String,Integer> entry: majorityMap.entrySet()){
					if(entry.getValue() > max){
						value=entry.getKey();
						max=entry.getValue();
					}
				}*/
				String value = keyValues.poll().value;
				if(!value.equals(FAILED_PORT_VALUE)){
					cursor.addRow(new String[]{key, value});
				}else{
					cursor.addRow(new String[]{key, keyValues.poll().value});
				}
				Log.d(TAG, "custom query method: final key value returned kry: "+key+" value: "+value);

			}
		} catch (InterruptedException e) {
			Log.e(TAG, "InterruptedException in custom query method");
		}

		return cursor;
	}


	public void breakLock(String key,String value){
		Log.d(TAG, "Entering breakLock Method");
		try {
			String[] valArray = value.split(",");
			Pair pair;
			Log.d(TAG, "Entering breakLock Method: value "+value);
			if(valArray.length==2) {
				pair = new Pair(key, valArray[0], Integer.parseInt(valArray[1]));
			}else{
				pair = new Pair(key, FAILED_PORT_VALUE, 0);
			}
			blockingQueue.put(pair);
		} catch (InterruptedException e) {
			Log.e(TAG, "InterruptedException in breakLock method");
		}
		Log.d(TAG, "Exiting breakLock Method");
	}

	public void querySuccessor(String key,String initialPort){
		Log.d(TAG, "querySuccessor at port: "+myPort+"for key: "+key);
		sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
				new Request(initialPort, BREAK_LOCK, key, sharedPreferences.getString(key, ""),null));
		Log.d(TAG, "querySuccessor to port "+initialPort);
		Log.d(TAG, "querySuccessor key: "+key);
		Log.d(TAG, "querySuccessor value "+sharedPreferences.getString(key, ""));
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private void setNeighbors(){
		Log.d(TAG, "set neighbors for "+myPort);
		int index = Arrays.asList(PORTS).indexOf(myPort);
		if(index < 3){
			successors[0]=PORTS[index+1];
			successors[1]=PORTS[index+2];
		}else if(index == 3){
			successors[0]=PORTS[4];
			successors[1]=PORTS[0];
		}else{
			successors[0]=PORTS[0];
			successors[1]=PORTS[1];
		}
		if(index > 1){
			predecessors[0]=PORTS[index-1];
			predecessors[1]=PORTS[index-2];
		}else if(index == 1){
			predecessors[0]=PORTS[0];
			predecessors[1]=PORTS[4];
		}else{
			predecessors[0]=PORTS[4];
			predecessors[1]=PORTS[3];
		}
		Log.d(TAG, "successors[0]"+successors[0]);
		Log.d(TAG, "successors[1]"+successors[1]);
		Log.d(TAG, "predecessors[0]"+predecessors[0]);
		Log.d(TAG, "predecessors[1]"+predecessors[1]);
	}

	public void getKeysPredecessor(String port){
		Log.d(TAG, "Entering getKeysPredecessor() at port: "+myPort);
		try {
		Map<String,String> keyMap = new HashMap<String,String>();
			sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
			Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
			String predHash = genHash(predecessors[0]);
			for (Map.Entry<String, String> entry : map.entrySet()) {
                String keyHash = genHash(entry.getKey());
				if(myPort.equals(FIRST_PORT)){
					if(keyHash.compareTo(nodeId) <0 || keyHash.compareTo(predHash) >0){
						Log.d(TAG, "getKeysPredecessor key: "+entry.getKey()+"to port: "+port);
						keyMap.put(entry.getKey(),entry.getValue());
					}
				}else if(keyHash.compareTo(nodeId) <0 && keyHash.compareTo(predHash) >0){
					keyMap.put(entry.getKey(),entry.getValue());
				}
            }
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(port,UPDATE_FROM_PRED, null, null,keyMap));
		} catch (NoSuchAlgorithmException e) {
			Log.d(TAG, "Entering getKeys() NoSuchAlgorithmException");
		}catch(Exception e){
			Log.d(TAG, "Exception occured in updateFromPredecessor ");
		}

	}

	public  void updateFromPredecessor(Map<String,String> keyMap){
		Log.d(TAG, "Entering updateFromPredecessor at port: " + myPort);
		sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
		Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
		for (Map.Entry<String, String> entry : keyMap.entrySet()) {
			SharedPreferences.Editor editor = sharedPreferences.edit();
			String key = entry.getKey();
			String value = entry.getValue();
			Log.d(TAG, "updateFromPredecessor key: " + key + " value: " + value);
			if (map.containsKey(key)) {
				String previousValue = sharedPreferences.getString(key, "");
				String[] prevValArray = previousValue.split(",");
				int previousVersion = Integer.parseInt(prevValArray[1]);
				String[] currentValArray = value.split(",");
				int currentVersion = Integer.parseInt(currentValArray[1]);
				if (previousVersion <= currentVersion) {
					editor.putString(key, value);
				}
			} else {
				editor.putString(key, value);
			}
			editor.commit();
		}
	}

	public void updateFromSuccessor(Map<String,String> keyMap){
		Log.d(TAG, "Entering updateFromSuccessor at port: " + myPort);
		sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
		Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
		for (Map.Entry<String, String> entry : keyMap.entrySet()) {
			SharedPreferences.Editor editor = sharedPreferences.edit();
			String key = entry.getKey();
			String value = entry.getValue();
			if (map.containsKey(key)) {
				String previousValue = sharedPreferences.getString(key, "");
				String[] prevValArray = previousValue.split(",");
				int previousVersion = Integer.parseInt(prevValArray[1]);
				String[] currentValArray = value.split(",");
				int currentVersion = Integer.parseInt(currentValArray[1]);
				if (previousVersion <= currentVersion) {
					editor.putString(key, value);
				}
				Log.d(TAG, "updateFromSuccessor, previous: " + previousValue + "current: " + value);
			} else {
				editor.putString(key, value);
			}
			editor.commit();
		}
		Log.d(TAG, "Exiting updateFromSuccessor at port: " + myPort + "map: " + keyMap);
	}

	public void getKeysSuccessor(String port,String predecessor){
		try {
		Log.d(TAG, "Entering getKeysSuccessor at port: "+myPort);
		Map<String,String> keyMap = new HashMap<String,String>();
			sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
			Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
			String nodeHash = genHash(port);
			String predecessorHash = genHash(predecessor);
			for (Map.Entry<String, String> entry : map.entrySet()) {
				String keyHash = genHash(entry.getKey());
				if(port.equals(FIRST_PORT)){
					if(keyHash.compareTo(nodeHash) <0 || keyHash.compareTo(predecessorHash) >0){
						keyMap.put(entry.getKey(),entry.getValue());
					}
				}else if(keyHash.compareTo(nodeHash) <0 && keyHash.compareTo(predecessorHash) >0){
					keyMap.put(entry.getKey(),entry.getValue());
				}
			}
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
					new Request(port,UPDATE_FROM_SUCC, null, null,keyMap));
		} catch (NoSuchAlgorithmException e) {
			Log.d(TAG, "Entering getKeys()");
		}
	}

	@Override
	public boolean onCreate() {
		Log.d(TAG, "Entering onCreateMethod");
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr)));

		try {
			nodeId = genHash(myPort);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "Exception while generating hashcode for emulator id");
		}
		Log.d(TAG, "onCreateMethod and port is : " + myPort);

		setNeighbors();
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException exc) {
			Log.e(TAG, exc.getMessage());
		}
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
				new Request(predecessors[0], GET_KEYS_PRED, null, myPort, null));
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
				new Request(predecessors[1], GET_KEYS_PRED, null, myPort, null));
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
				new Request(successors[0], GET_KEYS_SUCC, myPort, predecessors[0], null));


		return true;
	}


	public void failureHandle(Request request){
		try {
			String remotePort = request.port;
			Log.d(TAG, "Entering failure Handle method with remote port: "+remotePort);
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(remotePort)*2);

			OutputStream outputStream = socket.getOutputStream();
			ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
			objectOutputStream.writeObject(request);
			outputStream.flush();

			// Checking acknowledgment
			InputStream inputStream = socket.getInputStream();
			DataInputStream dataInputStream = new DataInputStream(inputStream);
			Log.d(TAG, "Client Task reading acknowledgement "+remotePort);
			if (dataInputStream.readUTF().equals(ACK)) {
				socket.close();
				Log.d(TAG, "Client Task socket closed"+remotePort);
			}

		} catch (IOException e) {
			Log.d(TAG, "IO Exception failureHandle method ");
		}
	}


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket clientSocket = null;

			//Waiting for incoming connections
			while (true) {
				try {
					//Accepting Request
					clientSocket = serverSocket.accept();
					InputStream inputStream = clientSocket.getInputStream();
					ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
					Request request = (Request)objectInputStream.readObject();
					String requestType = request.requestType;
					Log.d(TAG, "Inside Server Task with myPort as: "+myPort+" request.port"+request.port);
					Log.d(TAG, "Request Type is:"+requestType);

					if(requestType.equals(INSERT)){
						insert(request.key,request.value);
					}else if(requestType.equals(REP_INSERT)){
						replicateInsert(request.key,request.value);
					}else if(requestType.equals(DELETE)){
						delete(request.key);
					}else if(requestType.equals(REP_DELETE)){
						replicateDelete(request.key);
					}else if(requestType.equals(CLEAR_DATA)){
						clearData();
					}else if(requestType.equals(QUERY)){
						query(request.key,request.value);
					}else if(requestType.equals(BREAK_LOCK)){
						breakLock(request.key,request.value);
					}else if(requestType.equals(QUERY_ALL)){
						queryAll(request.value);
					}else if(requestType.equals(SET_GET_ALL_CURSOR)){
						fillGetAllCursor(request.map);
					}else if(requestType.equals(GET_KEYS_PRED)){
						getKeysPredecessor(request.value);
					}else if(requestType.equals(GET_KEYS_SUCC)){
						getKeysSuccessor(request.key,request.value);
					}else if(requestType.equals(QUERY_SUCC)){
						querySuccessor(request.key,request.value);
					}else if(requestType.equals(UPDATE_FROM_PRED)){
						updateFromPredecessor(request.map);
					}else if(requestType.equals(UPDATE_FROM_SUCC)){
						updateFromSuccessor(request.map);
					}else if(requestType.equals(BREAK_GET_ALL_LOCK)){
						breakGetAllLock();
					}

					//Sending acknowledgment
					OutputStream outputStream = clientSocket.getOutputStream();
					DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
					Log.d(TAG, "serverTask writing acknowledgement");
					dataOutputStream.writeUTF(ACK);
					Log.d(TAG, "serverTask acknowledgement written");
					dataOutputStream.flush();

					clientSocket.close();

				} catch (IOException exc) {
					Log.e(TAG, "IOException in Server Task");
				}catch (ClassNotFoundException exc) {
					Log.e(TAG, "Class Not Found Exception in Server Task");
				}

			}

		}
	}

	private class ClientTask extends AsyncTask<Request, Void, Void> {

		@Override
		protected Void doInBackground(Request... msgs) {
			Request request = msgs[0];
			try {
				String remotePort=null;
				remotePort = request.port;

				Log.d(TAG, "Client Task with emu Id "+remotePort+"request type: "+request.requestType);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(remotePort)*2);

				OutputStream outputStream = socket.getOutputStream();
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
				objectOutputStream.writeObject(request);
				outputStream.flush();

				// Checking acknowledgment
				InputStream inputStream = socket.getInputStream();
				DataInputStream dataInputStream = new DataInputStream(inputStream);
				Log.d(TAG, "Client Task reading acknowledgement "+remotePort);
				if (dataInputStream.readUTF().equals(ACK)) {
					socket.close();
					Log.d(TAG, "Client Task socket closed"+remotePort);
				}
			} catch (UnknownHostException exc) {
				Log.e(TAG,"UnknownHostException");
				exc.printStackTrace();
			} catch (IOException exc) {
				String requestType = request.requestType;
				if(requestType.equals(QUERY_SUCC)){
					Log.d(TAG, "Exception caught in client task for"+request.port+"and query: "+QUERY_SUCC);
					failureHandle(new Request(request.value,BREAK_LOCK,request.key,FAILED_PORT,null));
				}else if(requestType.equals(INSERT)){
					Log.d(TAG, "Exception caught in client task for"+request.port+"and query: "+INSERT);
                    for(Map.Entry<String,String> entry: request.map.entrySet()){
						Log.d(TAG, "failure handle called for"+entry.getValue());
						Log.d(TAG, "key sent for replication"+request.key);
						String val = request.value+","+0;
						failureHandle(new Request(entry.getValue(),REP_INSERT,request.key,val,null));
					}
				}else if(requestType.equals(QUERY)){
					Log.d(TAG, "Exception caught in client task for"+request.port+"and query: "+QUERY);
					for(Map.Entry<String,String> entry: request.map.entrySet()){
						Log.d(TAG, "failure handle called for"+entry.getValue());
						failureHandle(new Request(entry.getValue(),QUERY_SUCC,request.key,request.value,null));
					}
					failureHandle(new Request(request.value,BREAK_LOCK,request.key,FAILED_PORT,null));
				}else if(requestType.equals(QUERY_ALL)){
					Log.d(TAG, "Exception caught in client task for"+request.port+"and query: "+QUERY_ALL);
					failureHandle(new Request(request.value,BREAK_GET_ALL_LOCK,null,null,null));
				}else if(requestType.equals(GET_KEYS_PRED)){
					Log.d(TAG, "Exception caught in client task for"+request.port+"and query: "+GET_KEYS_PRED);
					Map<String,String> map = new HashMap<String, String>();
					failureHandle(new Request(request.value,UPDATE_FROM_PRED,null,null,map));
				}
				else if(requestType.equals(GET_KEYS_SUCC)){
					Log.d(TAG, "Exception caught in client task for"+request.port+"and query: "+GET_KEYS_SUCC);
					Map<String,String> map = new HashMap<String, String>();
					failureHandle(new Request(request.key,UPDATE_FROM_SUCC,null,null,map));
				}
				Log.e(TAG,"IOException");
				Log.d(TAG,"IOException");
				exc.printStackTrace();
			}

			return null;
		}
	}
}