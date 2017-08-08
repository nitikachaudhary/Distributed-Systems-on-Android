package edu.buffalo.cse.cse486586.simpledht;

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
import java.util.Formatter;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider{


    static final String START_NODE = "5554";
    static final int SERVER_PORT = 10000;
    public static final String MESSAGES="messages";
    public static final String KEY="key";
    public static final String VALUE="value";
    public static final String ACK="acknowledgement";

    public static final String JOIN="join";
    public static final String SET_NEIGHBORS="setNeighbors";

    public static final String INSERT="insert";
    public static final String QUERY="query";
    public static final String DELETE="delete";
    public static final String DELETE_ALL="deleteAll";
    public static final String GET_ALL ="getAll";
    public static final String SET_GET_ALL_CURSOR="setGetALlCursor";
    public static final String BREAK_LOCK="breakLock";
    public static final String BREAK_GET_ALL_LOCK="breakGetAllLock";

    BlockingQueue<String> blockingQueue = new LinkedBlockingDeque<String>();
    BlockingQueue<String> blockingQueueGetAll = new LinkedBlockingDeque<String>();
    MatrixCursor getAllCursor = new MatrixCursor(new String[]{KEY, VALUE});

    SharedPreferences sharedPreferences;


    String emulatorId;
    String nodeId;
    String predHashId;
    String succHashId;
    String predEmuId;
    String succEmuId;



    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
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
            deleteAll(emulatorId);
            Log.d(TAG, "overridden delete method when query is @ end");
        }else {
            Log.d(TAG, "overridden delete method when query is any key: start");
            delete(selection);
            Log.d(TAG, "overridden delete method when query is any key : end");
        }
        return 0;
    }

    public void deleteAll(String startNode){
        Log.d(TAG, "Entering custom deleteAll method");
        sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
        SharedPreferences.Editor editor = sharedPreferences.edit();
        editor.clear();
        editor.commit();
        if(!succEmuId.equals(startNode)){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    new Request(succEmuId,startNode,DELETE_ALL,null,null,null,null,null));
        }
    }

    public void delete(String key){
        Log.d(TAG, "Entering custom delete method ");
        int node = findNode(key);
        Log.d(TAG, "custom delete method value of  node: " +node);
        if(node == 0){
            sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
            SharedPreferences.Editor editor = sharedPreferences.edit();
            editor.remove(key);
            editor.commit();
            Log.v("The deleted key is", key);
        } else if(node == 1){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    new Request(succEmuId,null,DELETE,key,null,null,null,null));
        }else{
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    new Request(predEmuId,null,DELETE,key,null,null,null,null));
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.d(TAG, "Entering overridden insert method ");
        insert(values.getAsString(KEY),values.getAsString(VALUE));
        Log.d(TAG, "Exiting overridden insert method ");
        return uri;
    }

    public void insert(String key, String value){
        Log.d(TAG, "Entering custom insert method ");
        int node = findNode(key);
        if(node == 0){
            sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
            SharedPreferences.Editor editor = sharedPreferences.edit();
            editor.putString(key,value);
            editor.commit();
            Log.v("The inserted key is", key +"was inserted in emulator" + emulatorId);
        } else if(node == 1){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    new Request(succEmuId,null,INSERT,key,value,null,null,null));
        }else{
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    new Request(predEmuId,null,INSERT,key,value,null,null,null));
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.d(TAG, "Entering overridden query method ");
        // TODO Auto-generated method stub
        if (selection.equals("@")) {
            Log.d(TAG, "overridden query method when query is @ start");
            MatrixCursor cursor = new MatrixCursor(new String[]{KEY, VALUE});
            sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
            Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
            }
            //cursor.setNotificationUri(getContext().getContentResolver(),uri);
            Log.d(TAG, "overridden query method when query is @ end");
            Log.v("query", selection);
            return cursor;
        }else if (selection.equals("*")) {
            Log.d(TAG, "overridden query method when query is * start");
            return getAll(emulatorId);
        }
        else
            return query(selection,emulatorId);
    }
    public Cursor getAll(String startNode){
        try {
            Log.d(TAG, "entering getAll  method with emu ID "+emulatorId+" and start node" + startNode);
        sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
        Map<String, String> map = (Map<String, String>) sharedPreferences.getAll();
        if(emulatorId.equals(startNode)){
            if(map!=null) {
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    synchronized (getAllCursor) {
                        getAllCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                    }
                }
            }
            Log.d(TAG, "getAll  method: getallcursor has been set for start node by startnode");
        }else{
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    new Request(startNode, null, SET_GET_ALL_CURSOR, null, null, null,null,map));
            Log.d(TAG, "getAll  method: getallcursor has been set for start node by emulator" + emulatorId+"with map" +
                    "value: "+map);
        }
        if (!succEmuId.equals(startNode)) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        new Request(succEmuId, startNode, GET_ALL, null, null, null, null,null));
        }else{
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    new Request(startNode, null, BREAK_GET_ALL_LOCK, null, null, null, null,null));
            Log.d(TAG, "getAll  method: break_get_all_lock which means lock removed");
        }
            if(emulatorId.equals(startNode)){
                Log.d(TAG, "getAll  method: put the get all lock");
                blockingQueueGetAll.take();
                Log.d(TAG, "getAll  method: this is after the blockingQueueGetAll.take() thing");
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "InterruptedException in getAll method");
        }

        return getAllCursor;
    }

    public void fillGetAllCursor(Map<String,String> map){
        Log.d(TAG, "Entering fillGetAllCursor method");
        if(map!=null) {
            Log.d(TAG, "Entering fillGetAllCursor method map is not null");
            for (Map.Entry<String, String> entry : map.entrySet()) {
                synchronized (getAllCursor) {
                    getAllCursor.addRow(new String[]{entry.getKey(), entry.getValue()});
                }
            }
        }
        Log.d(TAG, "Exiting fillGetAllCursor method");
    }

    public void breakGetAllLock(){
        Log.d(TAG, "Entering breakGetAllLockMethod method");
        try {
            blockingQueueGetAll.put("BreakLock");
        } catch (InterruptedException e) {
            Log.e(TAG, "InterruptedException in getAll method");
        }
        Log.d(TAG, "Exiting breakGetAllLockMethod method");
    }

    public Cursor query(String key,String startNode){
        Log.d(TAG, "Entering custom query method");
            int node = findNode(key);
        Log.d(TAG, "custom query method after finding node the value of node: "+node);
            MatrixCursor cursor = new MatrixCursor(new String[]{KEY, VALUE});
            if (node == 0) {
                sharedPreferences = getContext().getSharedPreferences(MESSAGES, Context.MODE_PRIVATE);
                if(emulatorId.equals(startNode)) {
                    cursor.addRow(new String[]{key, sharedPreferences.getString(key, "")});
                    //cursor.setNotificationUri(getContext().getContentResolver(),uri);
                    Log.v("query", key);
                    Log.d(TAG, "custom query method: the key was found on the same node where request was made "+node);
                    return cursor;
                }
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        new Request(startNode, null, BREAK_LOCK, null, sharedPreferences.getString(key, ""), null, null,null));
                Log.d(TAG, "custom query method: this is after the client task for breaklock was called");

            } else if (node == 1) {
                Log.d(TAG, "custom query method: this is when QUERY is called for succ");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        new Request(succEmuId, startNode, QUERY, key, null, null, null,null));
            } else {
                Log.d(TAG, "custom query method: this is when QUERY is called for pred");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                        new Request(predEmuId, startNode, QUERY, key, null, null, null,null));
            }

        try {
            if(emulatorId.equals(startNode)) {
                Log.d(TAG, "custom query method: next stmt is blockingQueue.take() ");
                String value = blockingQueue.take();
                cursor.addRow(new String[]{key, value});
                Log.d(TAG, "custom query method: blockingQueue.take()  is broken and cursor key and value is "+key+" and"+ value);
            }
        } catch (InterruptedException e) {
            Log.e(TAG, "InterruptedException in custom query method");
        }
        return cursor;
    }

    public void breakLock(String value){
        Log.d(TAG, "Entering breakLock Method");
        try {
            blockingQueue.put(value);
        } catch (InterruptedException e) {
            Log.e(TAG, "InterruptedException in breakLock method");
        }
        Log.d(TAG, "Exiting breakLock Method");
    }

    public int findNode(String key){
        Log.d(TAG, "Entering findNode Method with emulator Id: "+ emulatorId+" and key: "+key);
        String keyHashId=null;
        try {
            keyHashId = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if(emulatorId.equals(predEmuId)) return 0;
        if(nodeId.compareTo(predHashId) < 0){
            if(keyHashId.compareTo(predHashId) >0 || keyHashId.compareTo(nodeId) <0){
                Log.d(TAG, "findNode Method with emulator Id: "+ emulatorId + "and emuHashId"+nodeId+" and keyId"+ key+"value returned is 0 one ");
                return 0;
            }
        }
        if(keyHashId.compareTo(nodeId) <0 && keyHashId.compareTo(predHashId) >0){  // this node is the correct node
            Log.d(TAG, "findNode Method with emulator Id: "+ emulatorId +" and keyId"+ key+"value returned is 0 two");
            return 0;
        }else if(keyHashId.compareTo(nodeId) >0){   // pass the search to successor
            Log.d(TAG, "findNode Method with emulator Id: "+ emulatorId +" and keyId"+ key+"value returned is 1");
            return 1;

        }else {
            Log.d(TAG, "findNode Method with emulator Id: "+ emulatorId + "and emuHashId"+nodeId+" and keyId"+ key+"value returned is 2");
            return 2;
        }// pass the search to predecessor
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Log.d(TAG, "Entering onCreateMethod");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        emulatorId = String.valueOf((Integer.parseInt(portStr)));

        try {
            nodeId = genHash(emulatorId);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Exception while generating hashcode for emulator id");
        }
        Log.d(TAG, "onCreateMethod and nodeId is : "+nodeId);
        predHashId = nodeId;
        succHashId = nodeId;
        predEmuId=emulatorId;
        succEmuId=emulatorId;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException exc) {
            Log.e(TAG, exc.getMessage());
        }

        if(!emulatorId.equals(START_NODE)) {
            Log.d(TAG, "onCreateMethod calling join from create with joiner ID: "+emulatorId);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Request(START_NODE, emulatorId, JOIN, null, null, null, null, null));
        }
        return true;
    }

    public void join(String joinerId){
        Log.d(TAG, "inside join for : "+emulatorId+"for joiner id: "+joinerId);
        try {
            String joinerHashId = genHash(joinerId);
            if(emulatorId.equals(predEmuId) && emulatorId.equals(succEmuId)){
                Log.d(TAG, "inside join 1 for : "+emulatorId+"with joiner id" +joinerId);
                predEmuId=joinerId;
                succEmuId=joinerId;
                predHashId=joinerHashId;
                succHashId=joinerHashId;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Request(joinerId,null,SET_NEIGHBORS,null,null,emulatorId,emulatorId,null));
            }else if(nodeId.compareTo(predHashId) < 0 && ((joinerHashId.compareTo(nodeId) > 0 && joinerHashId.compareTo(predHashId) > 0)||
                    joinerHashId.compareTo(nodeId) < 0 )){
                    Log.d(TAG, "inside join 2 for : "+emulatorId+"with joiner id" +joinerId);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Request(predEmuId, null, SET_NEIGHBORS, null, null, null, joinerId, null));
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Request(joinerId, null, SET_NEIGHBORS, null, null, predEmuId, emulatorId, null));
                    predEmuId = joinerId;
                    predHashId = joinerHashId;
            }else if( nodeId.compareTo(succHashId) > 0 &&((joinerHashId.compareTo(nodeId) < 0 && joinerHashId.compareTo(succHashId) < 0)||
                    joinerHashId.compareTo(nodeId) > 0 )){
                    Log.d(TAG, "inside join 3 for : "+emulatorId+"with joiner id" +joinerId);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Request(succEmuId, null, SET_NEIGHBORS, null, null, joinerId, null, null));
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Request(joinerId, null, SET_NEIGHBORS, null, null, emulatorId, succEmuId, null));
                    succEmuId = joinerId;
                    succHashId = joinerHashId;
            }else if( nodeId.compareTo(joinerHashId) < 0 && succHashId.compareTo(joinerHashId) > 0){
                Log.d(TAG, "inside join 4 for : "+emulatorId+"with joiner id" +joinerId);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Request(joinerId,null,SET_NEIGHBORS,null,null,emulatorId,succEmuId,null));
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Request(succEmuId,null,SET_NEIGHBORS,null,null,joinerId,null,null));
                succEmuId=joinerId;
                succHashId=joinerHashId;
            }else{
                Log.d(TAG, "inside join 5 for : "+emulatorId+"with joiner id" +joinerId);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Request(succEmuId,joinerId,JOIN,null,null,null,null,null));
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException in join method");
        }

    }

    public void setNeighbors(String pred, String succ){
        Log.d(TAG, "Entering setNeighBors method for emulator "+emulatorId+"with pred: "+pred +"and succ: "+succ);
        try {
            if(pred!=null) {
                predEmuId = pred;
                predHashId=genHash(pred);
            }
            if(succ!=null) {
                succEmuId = succ;
                succHashId = genHash(succ);
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException in setNeighbors method");
        }
        Log.d(TAG, "Exiting setNeighBors method");
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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
                    Log.d(TAG, "Insider Server Task with emuId as: "+request.emuId+" and requestType Id "+request.requestType);

                    Log.d(TAG, "Request Type is:"+requestType);

                    Log.d(TAG, "Emulator:"+emulatorId+"Succ: "+succEmuId+"Pred "+predEmuId);

                    if(requestType.equals(JOIN)){
                        Log.d(TAG, "join called for: "+request.joinerId+"join called at :" +emulatorId);
                        join(request.joinerId);
                    }else if(requestType.equals(SET_NEIGHBORS)){
                        setNeighbors(request.predEmuId,request.succEmuId);
                    }
                    else if(requestType.equals(INSERT)){
                        insert(request.key,request.value);
                    }else if(requestType.equals(QUERY)){
                        query(request.key,request.joinerId);
                    }else if(requestType.equals(DELETE)){
                        delete(request.key);
                    }else if(requestType.equals(DELETE_ALL)){
                        deleteAll(request.joinerId);
                    }else if(requestType.equals(BREAK_LOCK)){
                        breakLock(request.value);
                    }else if(requestType.equals(GET_ALL)){
                        getAll(request.joinerId);
                    }else if(requestType.equals(SET_GET_ALL_CURSOR)){
                        Log.d(TAG, "the fillGetAllCursor if block in server with map" +request.map);
                        fillGetAllCursor(request.map);
                    }else if(requestType.equals(BREAK_GET_ALL_LOCK)){
                        breakGetAllLock();
                    }

                    //Sending acknowledgment
                    OutputStream outputStream = clientSocket.getOutputStream();
                    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                    Log.d(TAG, "serverTask writing acknowledgement");
                    dataOutputStream.writeUTF(ACK);
                    Log.d(TAG, "serverTask acknowledgement written");


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
            try {
                Request request = msgs[0];
                String remotePort=null;
                remotePort = request.emuId;

                Log.d(TAG, "Client Task with emu Id "+remotePort);

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
                Log.e(TAG,"IOException");
                exc.printStackTrace();
            }

            return null;
        }
    }
}
