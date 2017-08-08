package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
    static final int SERVER_PORT = 10000;
    static final String ACK = "Acknowledgement";
    static final String MSG = "Message";
    static final String AGREED_PRIORITY = "Agreed Priority";
    static final int TIMEOUT = 500;
    static final String KEY = "key";
    static final String VALUE = "value";
    static final String PROVIDER_URI ="content://edu.buffalo.cse.cse486586.groupmessenger2.provider";

    String myPort=null;
    String failedPort=null;

    static int timestamp=0;
    int priorityNumber=0;

    // Object to be stored in buffered queue
    public class Message {
        String message;
        int priority;
        boolean isDeliverable;
        String portId;

        public Message(String message, int priority, boolean isDeliverable,String portId){
            this.message=message;
            this.priority=priority;
            this.isDeliverable=isDeliverable;
            this.portId=portId;
        }
    }

    //Queue to hold the buffered messages
    PriorityQueue<Message> holdBackQueue = new PriorityQueue<Message>(25,new Comparator<Message>(){
        public int compare(Message m1, Message m2){
            if(m1.priority<m2.priority) return -1;
            else if(m1.priority>m2.priority) return 1;
            else return 0;
        }
    });

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException exc) {
            Log.e(TAG, exc.getMessage());
            return;
        }

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button button = (Button)findViewById((R.id.button4));
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                TextView textView = (TextView) findViewById(R.id.textView1);
                textView.append("\t" + msg + "\n");
                editText.setText(""); // This is one way to reset the input box.
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket clientSocket = null;
            priorityNumber = Integer.parseInt(myPort);

            synchronized (holdBackQueue){
                Iterator<Message> iterator = holdBackQueue.iterator();
                Message msg=null;
                Log.d(TAG,"initial removal before queue size" +holdBackQueue.size());
                while (iterator.hasNext()) {
                    msg = iterator.next();
                    if (msg.isDeliverable) {
                        iterator.remove();
                        publishProgress(msg.message);
                    } else break;
                }
                Log.d(TAG,"initial removal later queue size" +holdBackQueue.size());
            }


            try {
                //Waiting for incoming connections
                while (true) {

                    //Reading message
                    clientSocket = serverSocket.accept();
                    InputStream inputStream = clientSocket.getInputStream();
                    DataInputStream dataInputStream = new DataInputStream(inputStream);
                    String messageString = dataInputStream.readUTF();
                    String[] messageArray = messageString.split(",");

                    //Output Stream for client
                    OutputStream outputStream = clientSocket.getOutputStream();
                    DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

                    if (messageArray[0].equals(MSG)) {
                        String message = messageArray[1];
                        String port = messageArray[2];
                        priorityNumber++;
                        Message messageObject = new Message(message, priorityNumber, false,port);
                        synchronized (holdBackQueue){
                            holdBackQueue.offer(messageObject);
                        }
                        Log.d(TAG, message);

                        //Sending proposed priority
                        dataOutputStream.writeInt(priorityNumber);

                    } else if (messageArray[0].equals(AGREED_PRIORITY)) {
                        int priority = Integer.parseInt(messageArray[1]);

                        //proposed priority greater than agreed priority
                        priorityNumber=priority+Integer.parseInt(myPort);
                        String message = messageArray[2];
                        Iterator<Message> iterator = holdBackQueue.iterator();
                        Message msg = null;
                        while (iterator.hasNext()) {
                            msg =iterator.next();
                            if (msg.message.equals(message)) {
                                iterator.remove();
                                break;
                            }
                        }
                        synchronized (holdBackQueue){
                            holdBackQueue.offer(new Message(message, priority, true,msg.portId));
                        }

                        iterator = holdBackQueue.iterator();
                        Log.d(TAG, "size before publishing the messages "+holdBackQueue.size());
                        while (iterator.hasNext()) {
                            msg = iterator.next();
                            if (msg.isDeliverable) {
                                iterator.remove();
                                publishProgress(msg.message);
                            } else break;
                        }
                        Log.d(TAG, "size after publishing the messages "+holdBackQueue.size());

                        //Sending Acknowledgement
                        dataOutputStream.writeUTF(ACK);
                    }
                    inputStream.close();
                    outputStream.flush();
                    outputStream.close();
                    clientSocket.close();
                }
            } catch (IOException exc) {
                Log.e(TAG, exc.getMessage());
            }
            return null;
        }


        protected void onProgressUpdate(String... strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived + "\t\n");


            Uri providerUri = Uri.parse(PROVIDER_URI);
            ContentValues contentValues = new ContentValues();
            contentValues.put(KEY,String.valueOf(timestamp++));
            contentValues.put(VALUE,strReceived);
            Uri newUri = getContentResolver().insert(providerUri,contentValues);

            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String remotePort=null;
                String msgToSend = msgs[0];
                String port = msgs[1];
                int maximumPriority = -1;
                for (int i = 0; i < 5; i++) {
                    try {
                        remotePort = REMOTE_PORTS[i];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        socket.setSoTimeout(TIMEOUT);

                        // Sending a message
                        OutputStream outputStream = socket.getOutputStream();
                        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                        String message = MSG + "," + msgToSend+","+port;
                        dataOutputStream.writeUTF(message);
                        outputStream.flush();

                        // Reading the proposed priorities
                        InputStream inputStream = socket.getInputStream();
                        DataInputStream dataInputStream = new DataInputStream(inputStream);
                        int proposedPriority = dataInputStream.readInt();
                        maximumPriority = Math.max(maximumPriority, proposedPriority);
                        outputStream.close();
                        inputStream.close();
                        socket.close();
                    }catch(UnknownHostException exc){
                         Log.e(TAG,exc.getMessage());
                    }catch (IOException exc){
                        failedPort=remotePort;
                        Log.d(TAG, "this is the failed port inside exception : "+failedPort);
                            Log.d(TAG, "size before deleting the messages "+holdBackQueue.size());
                            Iterator<Message> iterator = holdBackQueue.iterator();
                            while (iterator.hasNext()) {
                                Message msg = iterator.next();
                                if (msg.portId.equals(failedPort)) iterator.remove();
                            }
                            Log.d(TAG, "size after deleting the messages "+holdBackQueue.size());
                        Log.e(TAG,"Client IO Exception : failed port message");
                     }
                }
                for (int i = 0; i < 5; i++) {
                    try {
                        remotePort = REMOTE_PORTS[i];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort));
                        socket.setSoTimeout(TIMEOUT);
                        // Sending agreed priority
                        OutputStream outputStream = socket.getOutputStream();
                        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                        dataOutputStream.writeUTF(AGREED_PRIORITY + "," + maximumPriority + "," + msgToSend);
                        outputStream.flush();

                        // Reading acknowledgement
                        InputStream inputStream = socket.getInputStream();
                        DataInputStream dataInputStream = new DataInputStream(inputStream);
                        if (dataInputStream.readUTF().equals(ACK)) {
                            outputStream.close();
                            inputStream.close();
                            socket.close();
                        }
                     }catch(UnknownHostException exc){
                         Log.e(TAG,exc.getMessage());
                      }catch (IOException exc){
                        failedPort=remotePort;
                        Log.d(TAG, "this is the failed port inside exception : "+failedPort);
                            Log.d(TAG, "size before deleting the messages "+holdBackQueue.size());
                            Iterator<Message> iterator = holdBackQueue.iterator();
                            while (iterator.hasNext()) {
                                Message msg = iterator.next();
                                if (msg.portId.equals(failedPort)) iterator.remove();
                            }
                            Log.d(TAG, "size after deleting the messages "+holdBackQueue.size());
                        }
                        Log.e(TAG,"Client IO Exception : failed port message");
                }

            return null;
        }
    }

}
