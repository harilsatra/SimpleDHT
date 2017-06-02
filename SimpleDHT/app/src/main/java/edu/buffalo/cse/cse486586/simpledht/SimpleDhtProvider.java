package edu.buffalo.cse.cse486586.simpledht;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static java.lang.Integer.parseInt;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    //Hard-coding the 5 redirection ports.
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    List<String> keys = new ArrayList<String>();
    Comparator<Node> comparator = new NodeComparator();
    //public PriorityQueue<Node> node_list = new PriorityQueue<Node>(5,comparator);
    TreeSet<Node> node_list = new TreeSet<Node>(comparator);
    //int insert_count =0 ;
    //List<Node> node_list = new ArrayList<Node>();
    boolean single_node = false;
    Map<String,String> query_results = new HashMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(node_list.isEmpty()|| single_node){
            if(selection.equals("@")||selection.equals("*")){
                Iterator<String> itr = keys.iterator();
                while(itr.hasNext()){
                    File file = new File(itr.next());
                    file.delete();
                }
                keys.clear();
            }
            else{
                File file = new File(selection);
                file.delete();
                keys.remove(selection);
            }
        }
        else{
            if(selection.equals("@")) {
                Iterator<String> itr = keys.iterator();
                while(itr.hasNext()){
                    File file = new File(itr.next());
                    file.delete();
                }
                keys.clear();
            }
            else if(selection.equals("*")) {
                Iterator<String> itr = keys.iterator();
                while(itr.hasNext()){
                    File file = new File(itr.next());
                    file.delete();
                }
                keys.clear();
                Node deleter = new Node();
                deleter.msg = "Delete All";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, deleter);
                try {
                    Thread.sleep(600); //400
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else{
                File file = new File(selection);
                file.delete();
                keys.remove(selection);
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String filename = null; // The actual value in 'key' will be our file name.

        if(node_list.isEmpty() || single_node){
            filename = values.get("key").toString(); // The actual value in 'key' will be our file name.
            String string = values.get("value").toString(); // The actual value in 'value' will be the content of the file which has name equal to it's corresponding key.

            //The following snippet of code is taken from PA1 and is used to create internal storage file with key as the name and value as the content.
            FileOutputStream outputStream;

            try {
                outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
                keys.add(filename);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
        }
        else{
            Node get_info = new Node();
            filename = values.get("key").toString();
            String hash_file = new String();
            try {
                hash_file = genHash(filename);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            get_info.hash = hash_file;
            Node correct;
            if(get_info.hash.compareTo(node_list.last().hash) > 0){
                correct = node_list.first();
            }
            else {
                correct = node_list.ceiling(get_info);
            }
            String string = values.get("value").toString();
            //Log.v(TAG,correct.port);
            correct.key = filename;
            correct.value = string;
            correct.msg = "Insert";
            //Log.v(TAG,"Key "+correct.key+" should be inserted in "+correct.port);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, correct);
            try {
                Thread.sleep(500); //50
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel =  (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((parseInt(portStr) * 2));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        if(Integer.parseInt(myPort)!=11108){
            Log.v(TAG,"About to connect to 11108");
            Node newNode = new Node();
            newNode.port = myPort;
            newNode.msg = "New Node";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newNode);
        }
        for(int i=0; i<1 && Integer.parseInt(myPort)!=11108; i++){
            Node get_info = new Node();
            get_info.msg = "Provide Information";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, get_info);
        }
        /*else{
            StringBuffer s = new StringBuffer();
            Node nodes_info = new Node();
            nodes_info.msg = "Information";
            Iterator<Node> iter = node_list.iterator();
            while (iter.hasNext()){
                Node current = iter.next();
                s.append(current.port);
                s.append("-");
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodes_info);
        }*/


        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        if(node_list.isEmpty()|| single_node){
            if(selection.equals("@") || selection.equals("*")){
                Iterator<String> itr = keys.iterator();
                String[] columns = {"key","value"};
                MatrixCursor m = new MatrixCursor(columns);
                while(itr.hasNext()){
                    Log.v(TAG,"Special Parameter");
                    FileInputStream fis = null;
                    String sel = itr.next();
                    Log.v(TAG,sel);
                    try {
                        fis = getContext().openFileInput(sel);
                    } catch (FileNotFoundException e) {
                        Log.e(TAG, "FILE NOT FOUND");
                        //e.printStackTrace();
                    }
                    StringBuffer fileContent = new StringBuffer("");

                    byte[] buffer = new byte[1024];
                    int n;
                    try {
                        while ((n = fis.read(buffer)) != -1)
                        {
                            fileContent.append(new String(buffer, 0, n));
                        }
                    } catch (IOException e) {
                        Log.e(TAG, "COULD NOT READ THE FILE");
                        //e.printStackTrace();
                    }
                    String[] rows = {sel, String.valueOf(fileContent)};
                    m.addRow(rows);
                    //Log.v("query", selection);
                }
                return m;
            }
            else{
                FileInputStream fis = null;
                try {
                    fis = getContext().openFileInput(selection); //the parameter 'selection' specifies the criteria for selecting rows which in our case is the key and hence the filename.
                } catch (FileNotFoundException e) {
                    Log.e(TAG, "FILE NOT FOUND");
                    //e.printStackTrace();
                }
                StringBuffer fileContent = new StringBuffer("");

                byte[] buffer = new byte[1024];
                int n;
                try {
                    while ((n = fis.read(buffer)) != -1)
                    {
                        fileContent.append(new String(buffer, 0, n));
                    }
                } catch (IOException e) {
                    Log.e(TAG, "COULD NOT READ THE FILE");
                    //e.printStackTrace();
                }

                //The matrix cursor constructor takes the names of the columns as a parameter, which in our case will be 'key' and 'value'
                String[] columns = {"key","value"};
                MatrixCursor m = new MatrixCursor(columns);
                //The addRow function adds a new row to the end with the given column values in the same order as mentioned while calling the constructor.
                //We add the key (parameter 'selection') and the content read from the file above as the key value pair in the matrix cursor.
                String[] rows = {selection, String.valueOf(fileContent)};
                m.addRow(rows);
                //Log.v("query", selection);
                return m;
            }

        }
        else{
            if(selection.equals("@")){
                Log.v(TAG,"Local STAR Query");
                Iterator<String> itr = keys.iterator();
                String[] columns = {"key","value"};
                MatrixCursor m = new MatrixCursor(columns);
                while(itr.hasNext()){
                    FileInputStream fis = null;
                    String sel = itr.next();
                    try {
                        fis = getContext().openFileInput(sel);
                    } catch (FileNotFoundException e) {
                        Log.e(TAG, "FILE NOT FOUND");
                        //e.printStackTrace();
                    }
                    StringBuffer fileContent = new StringBuffer("");

                    byte[] buffer = new byte[1024];
                    int n;
                    try {
                        while ((n = fis.read(buffer)) != -1)
                        {
                            fileContent.append(new String(buffer, 0, n));
                        }
                    } catch (IOException e) {
                        Log.e(TAG, "COULD NOT READ THE FILE");
                        //e.printStackTrace();
                    }
                    String[] rows = {sel, String.valueOf(fileContent)};
                    m.addRow(rows);
                    //Log.v("query", selection);
                }
                return m;
            }
            else if(selection.equals("*")){
                Log.v(TAG,"STAR QUERY");
                Node query_info = new Node();
                query_info.query = selection;
                query_info.msg = "Query All";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_info);
                try {
                    Thread.sleep(600); //400
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //Log.v(TAG,"Value for key "+selection+" is "+query_results.get(selection));
                //The matrix cursor constructor takes the names of the columns as a parameter, which in our case will be 'key' and 'value'
                String[] columns = {"key","value"};
                MatrixCursor m = new MatrixCursor(columns);
                //The addRow function adds a new row to the end with the given column values in the same order as mentioned while calling the constructor.
                //We add the key (parameter 'selection') and the content read from the file above as the key value pair in the matrix cursor.
                for (Map.Entry<String, String> entry : query_results.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] rows = {key, value};
                    m.addRow(rows);
                    Log.v(TAG,"Value for key "+key+" is "+value);
                }
                query_results.clear();
                //query_results.remove(selection);
                //Log.v("query", selection);
                Log.v(TAG,""+m.getCount());
                return m;
            }
            else{
                Node query_info = new Node();
                String query_hash = null;
                try {
                    query_hash = genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                query_info.hash = query_hash;
                Node destination;
                if(query_info.hash.compareTo(node_list.last().hash) > 0){
                    destination = node_list.first();
                }
                else {
                    destination = node_list.ceiling(query_info);
                }
                destination.query = selection;
                destination.msg = "Query";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, destination);
                try {
                    Thread.sleep(500); //100
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Log.v(TAG,"Value for key "+selection+" is "+query_results.get(selection));
                //The matrix cursor constructor takes the names of the columns as a parameter, which in our case will be 'key' and 'value'
                String[] columns = {"key","value"};
                MatrixCursor m = new MatrixCursor(columns);
                //The addRow function adds a new row to the end with the given column values in the same order as mentioned while calling the constructor.
                //We add the key (parameter 'selection') and the content read from the file above as the key value pair in the matrix cursor.
                String[] rows = {selection, query_results.get(selection)};
                query_results.remove(selection);
                m.addRow(rows);
                //Log.v("query", selection);
                return m;
            }
        }

        // TODO Auto-generated method stub
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
    private String genPort() {
        TelephonyManager tel =  (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((parseInt(portStr) * 2));
        return myPort;
    }
    private class ServerTask extends AsyncTask<ServerSocket, String, Void>{
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try{
                Socket socket;
                ObjectInputStream objFromClient;
                ObjectOutputStream ServerToClient;
                while(true){
                    socket = serverSocket.accept();
                    //Read the message over the socket
                    objFromClient = new ObjectInputStream(socket.getInputStream());
                    ServerToClient = new ObjectOutputStream(socket.getOutputStream());
                    Node in_node = null;
                    try {
                        //Log.v(TAG,"Waiting to get a message");
                        in_node = (Node) objFromClient.readObject();
                        //Log.v(TAG,"Read the object from client "+in_node.msg);
                    } catch (ClassNotFoundException e) {
                        Log.e(TAG,"ClassNotFoundException in Server Class");
                        e.printStackTrace();
                    }
                    //node_list.add(in_node);
                    if(in_node.msg.equals("New Node")){
                        in_node.hash = genHash(String.valueOf(Integer.parseInt(in_node.port)/2));
                        node_list.add(in_node);
                        //Log.v(TAG,"Added port "+in_node.port+" to live list");
                        in_node.msg = "Ack5";
                        ServerToClient.writeObject(in_node);
                        ServerToClient.flush();
                    }
                    if((in_node.msg.equals("Provide Information"))){
                        StringBuffer s = new StringBuffer();
                        in_node.msg = "Information";
                        Iterator<Node> iter = node_list.iterator();
                        while (iter.hasNext()){
                            Node current = iter.next();
                            s.append(current.port);
                            s.append("-");
                        }
                        s.append(REMOTE_PORT0);
                        in_node.info = s.toString();
                        ServerToClient.writeObject(in_node);
                        ServerToClient.flush();
                    }
                    if(in_node.msg.equals("Information")){
                        String[] live = in_node.info.split("-");
                        for(int i=0; i<live.length;i++){
                            Node z = new Node();
                            z.port = live[i];
                            z.hash = genHash(String.valueOf(Integer.parseInt(live[i])/2));
                            node_list.add(z);
                            //Log.v(TAG,"Added port "+z.port+" to live list");
                        }
                        in_node.msg = "Ack20";
                        ServerToClient.writeObject(in_node);
                        ServerToClient.flush();
                    }
                    if(in_node.msg.equals("Insert")){
                        keys.add(in_node.key);
                        FileOutputStream outputStream;
                        try {
                            outputStream = getContext().openFileOutput(in_node.key, Context.MODE_PRIVATE);
                            outputStream.write(in_node.value.getBytes());
                            outputStream.close();
                            Log.v(TAG,"Key "+in_node.key+" Inserted in "+in_node.port+ "hash = "+in_node.hash);
                        } catch (Exception e) {
                            Log.e(TAG, "File write failed");
                        }
                        in_node.msg = "Ack10";
                        ServerToClient.writeObject(in_node);
                        ServerToClient.flush();
                    }
                    if(in_node.msg.equals("Query")){
                        FileInputStream fis = null;
                        try {
                            fis = getContext().openFileInput(in_node.query); //the parameter 'selection' specifies the criteria for selecting rows which in our case is the key and hence the filename.
                        } catch (FileNotFoundException e) {
                            Log.e(TAG, "FILE NOT FOUND");
                            //e.printStackTrace();
                        }
                        StringBuffer fileContent = new StringBuffer("");

                        byte[] buffer = new byte[1024];
                        int n;
                        try {
                            while ((n = fis.read(buffer)) != -1)
                            {
                                fileContent.append(new String(buffer, 0, n));
                            }
                        } catch (IOException e) {
                            Log.e(TAG, "COULD NOT READ THE FILE");
                            //e.printStackTrace();
                        }
                        in_node.query_result = String.valueOf(fileContent);
                        in_node.msg = "Query_value";
                        ServerToClient.writeObject(in_node);
                        ServerToClient.flush();
                    }
                    if(in_node.msg.equals("Query All")) {
                        Iterator<String> itr = keys.iterator();
                        StringBuffer key = new StringBuffer();
                        StringBuffer value = new StringBuffer();
                        while (itr.hasNext()) {
                            //Log.v(TAG, "Special Parameter");
                            FileInputStream fis = null;
                            String sel = itr.next();
                            //Log.v(TAG, sel);
                            try {
                                fis = getContext().openFileInput(sel);
                            } catch (FileNotFoundException e) {
                                Log.e(TAG, "FILE NOT FOUND");
                                //e.printStackTrace();
                            }
                            StringBuffer fileContent = new StringBuffer("");

                            byte[] buffer = new byte[1024];
                            int n;
                            try {
                                while ((n = fis.read(buffer)) != -1) {
                                    fileContent.append(new String(buffer, 0, n));
                                }
                            } catch (IOException e) {
                                Log.e(TAG, "COULD NOT READ THE FILE");
                                //e.printStackTrace();
                            }
                            key.append(sel);
                            key.append("-");
                            value.append(String.valueOf(fileContent));
                            value.append("-");
                            //String[] rows = {sel, String.valueOf(fileContent)};
                        }
                        in_node.query = String.valueOf(key);
                        in_node.query_result = String.valueOf(value);
                        in_node.msg = "Ack*";
                        ServerToClient.writeObject(in_node);
                        ServerToClient.flush();
                    }
                    if(in_node.msg.equals("Delete All")){
                        Iterator<String> itr = keys.iterator();
                        while(itr.hasNext()){
                            File file = new File(itr.next());
                            file.delete();
                        }
                        keys.clear();
                        in_node.msg = "Delete*";
                        ServerToClient.writeObject(in_node);
                        ServerToClient.flush();
                    }
                    /*if(in_node.msg.equals("Information")){
                        in_node.hash = genHash(in_node.port);
                        node_list.add(in_node);
                        Log.v(TAG,"Added port "+in_node.port+" to live list");
                        in_node.msg = "Ack10";
                        ServerToClient.writeObject(in_node);
                        ServerToClient.flush();
                    }*/
                }
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, "Unknown Exception");
            }
            return null;
        }
    }
    private class ClientTask extends AsyncTask<Node, Void, Void> {
        @Override
        protected Void doInBackground(Node... nodes){
            Node n = nodes[0];
            //Log.v(TAG,"Inside ClientTask");
            if(n.msg.equals("New Node")|| n.msg.equals("Provide Information") ) {
                try{
                    //Log.v(TAG,"Connecting to 11108");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            parseInt(REMOTE_PORT0));
                    socket.setSoTimeout(500);
                    ObjectOutputStream objToServer = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream objFromServer = new ObjectInputStream(socket.getInputStream());
                    objToServer.writeObject(n);
                    objToServer.flush();
                    Node ack = null;
                    try {
                        ack = (Node) objFromServer.readObject();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    if (ack.msg.equals("Ack5")) {
                        //Log.v(TAG,"Message: "+ack.msg);
                        objToServer.close();
                        objFromServer.close();
                        socket.close();
                        //Log.v(TAG,"Closing all the resources");
                    }

                    if (ack.msg.equals("Information")){
                        String[] live = ack.info.split("-");
                        for(int i=0; i<live.length;i++){
                            Node z = new Node();
                            z.port = live[i];
                            z.hash = genHash(String.valueOf(Integer.parseInt(live[i])/2));
                            node_list.add(z);
                            //Log.v(TAG,"Added port "+z.port+" to live list");
                        }
                        objToServer.close();
                        objFromServer.close();
                        socket.close();
                        //Log.v(TAG,"Closing all the resources");
                        for(int i=0; i<live.length;i++){
                            Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    parseInt(live[i]));
                            ObjectOutputStream objToServer1 = new ObjectOutputStream(socket1.getOutputStream());
                            ObjectInputStream objFromServer1 = new ObjectInputStream(socket1.getInputStream());
                            StringBuffer s = new StringBuffer();
                            Node temp = new Node();
                            temp.msg = "Information";
                            Iterator<Node> iter = node_list.iterator();
                            while (iter.hasNext()){
                                Node current = iter.next();
                                s.append(current.port);
                                s.append("-");
                            }
                            temp.info = s.toString();
                            objToServer1.writeObject(temp);
                            objToServer1.flush();
                            Node ack1 = null;
                            try {
                                ack1 = (Node) objFromServer1.readObject();
                            } catch (ClassNotFoundException e) {
                                e.printStackTrace();
                            }
                            if (ack1.msg.equals("Ack20")) {
                                //Log.v(TAG,"Message: "+ack1.msg);
                                objToServer1.close();
                                objFromServer1.close();
                                socket1.close();
                                //Log.v(TAG,"Closing all the resources");
                            }
                        }

                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    single_node = true;
                    Log.v(TAG,"Random Single Node");
                    //e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
            if(n.msg.equals("Insert") || n.msg.equals("Query")){
                //Log.v(TAG,"Instructing "+n.port+" to insert"+n.key);
                try{
                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            parseInt(n.port));
                    ObjectOutputStream objToServer2 = new ObjectOutputStream(socket2.getOutputStream());
                    ObjectInputStream objFromServer2 = new ObjectInputStream(socket2.getInputStream());
                    objToServer2.writeObject(n);
                    objToServer2.flush();
                    Node ack = null;
                    try {
                        ack = (Node) objFromServer2.readObject();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    if (ack.msg.equals("Ack10") && ack.key.equals(n.key)) {
                        //Log.v(TAG,"Message: "+ack.msg);
                        objToServer2.close();
                        objFromServer2.close();
                        socket2.close();
                        //Log.v(TAG,"Closing all the resources");
                    }
                    if (ack.msg.equals("Query_value")){
                        query_results.put(ack.query,ack.query_result);
                        Log.v(TAG,"Value for key "+ack.query+" is "+query_results.get(ack.query));
                        objToServer2.close();
                        objFromServer2.close();
                        socket2.close();
                    }
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(n.msg.equals("Query All") || n.msg.equals("Delete All")){
                Iterator<Node> iter = node_list.iterator();
                while (iter.hasNext()){
                    Node current = iter.next();
                    try{

                        Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                parseInt(current.port));
                        ObjectOutputStream objToServer3 = new ObjectOutputStream(socket3.getOutputStream());
                        ObjectInputStream objFromServer3 = new ObjectInputStream(socket3.getInputStream());
                        objToServer3.writeObject(n);
                        objToServer3.flush();
                        Node ack = null;
                        try {
                            ack = (Node) objFromServer3.readObject();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                        if (ack.msg.equals("Ack*")){
                            String[] local_keys = ack.query.split("-");
                            String[] local_values = ack.query_result.split("-");
                            for(int i=0; i<local_keys.length; i++){
                                query_results.put(local_keys[i],local_values[i]);
                            }
                            objToServer3.close();
                            objFromServer3.close();
                            socket3.close();
                        }
                        if (ack.msg.equals("Delete*")){
                            objToServer3.close();
                            objFromServer3.close();
                            socket3.close();
                        }
                    }catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }
            return null;
        }
    }
}
