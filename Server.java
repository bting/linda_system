import com.sun.org.apache.xpath.internal.operations.Bool;
import javafx.beans.value.ChangeListener;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class Server extends Thread {

    /**
     * global variables
     */
    // The port and IP that the server listens on.
    private InetAddress IP;
    private int PORT;
    // login and host's name. indicate the path to store files
    private String login;
    private String name;
    private List<String> tuples = new ArrayList<>();

    public Server(String l, String n) {
        login = l;
        name = n;
    }

    /**
     * get and pass the information of the server to client.
     */
    public String getServerInfo() {
        String str = IP.getHostAddress() + " " + PORT;
        return str;
    }

    private File getDataFolder() {
        String path = "/tmp/" + login + "/linda/" + name;
        File folder = new File(path);
        return folder;
    }

    /**
     * recover after crash or killed
     */
    private void recover(int port) throws Exception {
       // update net file
       List<ServerItem> serverList = ServerList.loadServerList(login, name);
       int nodeID = 0;
       for (ServerItem server : serverList) {
           if (server.getName().equals(name)) {
               server.setPort(port);
               break;
           }
           nodeID += 1;
       }
       ServerList.saveServerList(login, name, serverList);
       ConsistentHash.updateTables(serverList.size());
       Client client = new Client(login, name);
       client.runClient(IP.getHostAddress(), PORT, "syncServerList");

       // clean up all old tuples
       TupleSpace.removeTupleFile(login, name);

       recoverFromBackup(serverList, nodeID);
       recoverFromPrimary(serverList, nodeID);
    }

    /**
     * read all tuples that should be originally stored in this node from back up node
     */
    private void recoverFromBackup(List<ServerItem> serverList, int nodeID) throws Exception {
        Set<Integer> backupNodes = ConsistentHash.getBackupNodes(nodeID);
        for (Integer ID : backupNodes) {
            Client client = new Client(login, name);
            ServerItem server = serverList.get(ID);
            client.runClient(server.getIP(), server.getPort(), "recover origin " + ID + " " + nodeID);
        }
    }

    /**
     * read all tuples that should be backed up in this node from primary nodes
     */
    private void recoverFromPrimary(List<ServerItem> serverList, int nodeID) throws Exception {
        Set<Integer> primaryNodes = ConsistentHash.getPrimaryNodes(nodeID);
        for (Integer ID : primaryNodes) {
            Client client = new Client(login, name);
            ServerItem server = serverList.get(ID);
            client.runClient(server.getIP(), server.getPort(), "recover backup " + ID + " " + nodeID);
        }
    }

    /**
     * Read all tuples that should be backed up in this node from origin node
     */

    /**
     * initiate server, let server connect to its own client
     */
    private void init(String IP, int port) throws Exception {
        if(getDataFolder().exists()) {
            // recover from crash or killed
            recover(port);
        } else {
            List<ServerItem> serverList = new ArrayList<>();
            ServerItem server = new ServerItem(name, IP, port);
            serverList.add(server);
            ServerList.saveServerList(login, name, serverList);
            TupleSpace.saveTupleFile(tuples, login, name);
            tuples = TupleSpace.loadTupleFile(login, name);
        }
    }

    /**
     * implement add-command, two host add each other.
     * command like this add 129.210.16.80 9998
     */
    private synchronized void addHandler(BufferedReader in) throws Exception {
        String input = in.readLine();
        String[] strs = input.split(" ");
        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        HashMap<String, ServerItem> hostMap = ServerList.getHostNameMap(login, name, serverList);
        for (int i = 0; i < strs.length; i = i+3) {
            String remoteHostName = strs[i];
            if (hostMap.containsKey(remoteHostName)) {
                continue;
            }
            String remoteIP = strs[i+1];
            int remotePort = Integer.parseInt(strs[i+2]);
            ServerItem newServer = new ServerItem(remoteHostName, remoteIP, remotePort);
            serverList.add(newServer);
        }
        ServerList.saveServerList(login, name, serverList);

        // inform all the servers in server list to update their server_list
        Client client = new Client(login, name);
        client.runClient(IP.getHostAddress(), PORT, "forward");
    }

    /**
     * Implement handler for forward command
     */
    private synchronized void forwardHandler(BufferedReader in) throws IOException {
        String input;
        String path = "/tmp/"+login+"/linda/"+name+"/nets";
        BufferedWriter bw = new BufferedWriter(new FileWriter(path));
        while ((input = in.readLine())!=null) {
            if(input.length() != 0) {
                bw.write(input + "\n");
            }
        }
        bw.close();
    }

    private synchronized boolean isExactMatched(String tupleStr, boolean isRemove) {
        String target = TupleSpace.createMatchedTupleFromString(tupleStr);
        if (tuples.size() != 0) {
            for (String t: tuples) {
                String[] strs = t.split("&");
                if (strs[0].equals(target)) {
                    if (isRemove) {
                        tuples.remove(t);
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     *If target tuple has variable, use this function.
     *check whether tuple space has a matched tuple or not,
     * If yes, return one of that.
     */
    private String isVariableMatched(String tupleStr, boolean isRemove) {
        /* check tuple's size and tupleStr's size
        then put qualified candidates into Set.*/
        String result = "";
        String[] strs = tupleStr.split(",");
        ArrayList<String> matched = new ArrayList<>();
        if (tuples.size() == 0) {
            return "";
        }

        for(String t: tuples) {
            String[] source = t.split(":", 3);
            String[] tempTuple = source[2].split("&");
            String[] splitTuples = tempTuple[0].substring(1, tempTuple[0].length()-1).split(",");
            int count = Integer.parseInt(source[0].trim());
            if (count != strs.length) {
                continue;
            }
            String type = TupleSpace.getTypesOfTuple(strs);
            if (!type.equals(source[1].trim())) {
                continue;
            }
            boolean isMatched = true;
            for (int i = 0; i < strs.length; i++) {
                String temp = strs[i].trim();
                //System.out.println("debug2 "+ t + " : " + temp);
                if (temp.startsWith("?")) {
                    //System.out.println("debug " + temp);
                    continue;
                } else {
                    if (!splitTuples[i].equals(temp)) {
                        System.out.println(splitTuples[i] + " : " + temp);
                        isMatched = false;
                        break;
                    }
                }
            }
            if (isMatched == true) {
                matched.add(t);
            }
        }

        // If have matched tuples, return one of them
        if (matched.size() > 0) {
            String find = matched.get(matched.size()-1);
            if (isRemove) {
                tuples.remove(find);
            }
            String[] tempFind = find.split(":", 3);
            //String[] results = tempFind[2].trim().split("&");
            result = tempFind[2];
            //System.out.println("variable match result: " + result);
        }
        return result;
    }

    /**
     * according to whether tuple target has variable or not
     * hand out to different functions.(isVariableMatched OR isExactMatched)
     */
    private String checkTuple(BufferedReader in, boolean isRemove) throws IOException {
        String isVal = in.readLine();
        String tupleStr = in.readLine().trim();
        String response = "";
        if (isVal.equals("true")) {
            String s1 = isVariableMatched(tupleStr, isRemove);
            if (s1.length() != 0) {
                response = s1;
            }
        } else {
            boolean isMatched = isExactMatched(tupleStr, isRemove);
            if (isMatched) {
                response = "get Tuple (" + tupleStr + ") on " + name + " : " + IP.getHostAddress();
            }
        }
        return response;
    }

    /**
     * implement in-command,The	“in" will match	and	remove tuples from TS
     * for example: in(“abc”, ?i:int)
     */
    private String inHandler(BufferedReader in) throws IOException {
        boolean isRemove = true;
        String reponse = checkTuple(in, isRemove);
        TupleSpace.saveTupleFile(tuples, login, name);
        return reponse;
    }

    /**
     * implement out-command, The “out”	simply put tuples in TS
     * for example out(“abc”, 3)
     */
    private String outHandler(BufferedReader in) throws IOException {
        String tupleStr = in.readLine();
        String hashVal = in.readLine();
        String flag = in.readLine();
        String response = "";
        if (tupleStr != null && tupleStr.length() > 0) {
            String tuple = TupleSpace.createTupleFromString(tupleStr, hashVal, flag);
            TupleSpace.appendToTupleFile(tuple, login, name);
            tuples.add(tuple);
            response = "Put tuple " +  "(" + tupleStr + ") on " + name + " : " + IP.getHostAddress();
        }
        return response;
    }

    /**
     * implement rd-command, the "rd" will match but not remove tuples from TS
     */
    private String rdHandler(BufferedReader in) throws IOException {
        boolean isRemove = false;
        String response = checkTuple(in, isRemove);
        return response;
    }

    /**
     * check the current host is master or not
     * @param in
     * @return
     * @throws IOException
     */

    private String checkHandler(BufferedReader in) throws IOException {
        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        if (serverList.size() > 1) {
            return "true";
        } else {
            return "false";
        }
    }

    /**
     * get update request from deleted host to update serverList.
     * the newest serverList is passed in string format.
     * @param in
     * @throws IOException
     */
    private void updateServerListHandler(BufferedReader in) throws IOException {
        String serverInfo = in.readLine();
        if (serverInfo.length() == 0) {
            System.out.println("Error happens when executing delete request.");
        }
        String[] serverInfos = serverInfo.split(" ");
        List<ServerItem> newList = new ArrayList<>();
        for (int i = 0; i < serverInfos.length; i = i+3) {
            String serverName = serverInfos[i];
            String serverIp = serverInfos[i+1];
            int serverPort = Integer.parseInt(serverInfos[i+2]);
            ServerItem newServer = new ServerItem(serverName, serverIp, serverPort);
            newList.add(newServer);
        }
        ServerList.saveServerList(login, name, newList);
    }

    /**
     * the removed host need to transfer its data
     * get the newList host info in string format
     * need to check every tuple and find out which new host it should go to
     * @param in
     * @throws Exception
     */
    private void transferTupleHandler(BufferedReader in) throws Exception {
        String newServerList = in.readLine();
        if (newServerList.length() < 1) {
            System.out.println("Error happens when try to delete host " + name);
        }

        // pass newServerList to current removed host's client
        // and let it transfer tuple to particular host.
        Client client = new Client(login, name);
        String subCommand = "remove&" + newServerList;
        client.runClient(IP.getHostAddress(), PORT, subCommand);
    }

    /**
     * after the host has been removed, need to remove its net and tuple files
     * @throws IOException
     */
    private void cleanUp() throws IOException {
        ServerList.removeServerFile(login, name);
        TupleSpace.removeTupleFile(login, name);
        String folderPath = "/tmp/" + login + "/linda/" + name;
        File folder = new File(folderPath);
        folder.delete();
        System.out.println("Host " + name + " has been deleted and folder " + folderPath + " has been removed");
        System.out.println("Exiting now, bye!");
        System.exit(0);
    }

    /**
     * store the tuples from removed host.
     * this tuple is already in the converted format.
     * @param in
     * @throws IOException
     */
    private void redistributeHandler(BufferedReader in) throws IOException {
        String tupleStr = in.readLine();
        String hashVal = in.readLine();
        String flag = in.readLine();
        //String response = "";
        if (tupleStr.length() == 0 || hashVal.length() == 0) {
            System.out.println("Errors happens when try to redistribute " + tupleStr);
            return;
        }
        String tuple = tupleStr+"&"+hashVal + "&" + flag;
        TupleSpace.appendToTupleFile(tuple, login, name);
    }

    /**
     * delete all the same tuples of removed host in other left hosts
     * @param in
     * @throws IOException
     */
    private void deleteSameTuples(BufferedReader in) throws IOException {
        String deletedTuple = in.readLine();
        String flag = in.readLine();
        if (deletedTuple == null || deletedTuple.length() == 0) {
            System.out.println("Errors happens when deleting host to move backup tuple: " + deletedTuple);
        }
        deletedTuple = deletedTuple + "&" + flag;
        List<String> wholeTuples = TupleSpace.loadTupleFile(login, name);
        for (String origin: wholeTuples) {
            //System.out.println(origin + " : " + deletedTuple);
            if (origin.equals(deletedTuple)) {
                wholeTuples.remove(origin);
                //System.out.println(origin + " has been deleted from " + name);
                break;
            }
        }
        TupleSpace.saveTupleFile(wholeTuples, login, name);
    }

    /**
     * After add and delete operations, update all the tuples of the newest ServerList
     * if the tuples are in the right host, leave it
     * select all the tuples needed to remove, parse them into string
     * and then send request to client to inform particular host to store the removed tuple
     * @throws Exception
     * save the newest tuple List!!!
     */

    private void updateTupleAfterAllHandler() throws Exception {
        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        ConsistentHash.updateLookUpTable(serverList.size());
        ConsistentHash.updateBackUpTable();
        List<String> totaltuples = TupleSpace.loadTupleFile(login, name);
        List<String> newTupleList = totaltuples;
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < totaltuples.size(); i++) {
            String tuple = totaltuples.get(i);
            String[] tupleInfo = tuple.split("&");
            int hashVal = Integer.parseInt(tupleInfo[1]);
            String flag = tupleInfo[2];
            int[] ids = ConsistentHash.getIds(hashVal);

            if (flag.equals("origin")) {
                String hostName = serverList.get(ids[0]).getName();
                if (!hostName.equals(name)) {
                    newTupleList.remove(tuple);
                    sb.append(tuple + " ");
                }
            } else if (flag.equals("backup")){
                String backUpName = serverList.get(ids[1]).getName();
                if(!backUpName.equals(name)) {
                    newTupleList.remove(tuple);
                    sb.append(tuple + " ");
                }
            }
        }
        TupleSpace.saveTupleFile(newTupleList,login, name);
        // inform all the servers in server list to update their server_list
        Client client = new Client(login, name);
        client.runClient(IP.getHostAddress(), PORT, "moveTupleAfterAll " + sb.toString());
    }

    /**
     * After add and delete operation, get the updated tuple from newest serverList
     * @param in perfect formatted tuple
     * @throws IOException
     */
    private void getUpdateTupleHandler(BufferedReader in) throws IOException {
        String tuple = in.readLine();
        if (tuple == null || tuple.length() == 0) {
            System.out.println("Error happens when try to move tuple " + tuple + " on host " + name);
        }
        TupleSpace.appendToTupleFile(tuple, login, name);
    }

    /**
     * handle recover request: go through tuple list and find out corresponding tuples
     */
    private void recoverHandler(BufferedReader in) throws Exception {
        String flag = in.readLine();
        int nodeID = Integer.parseInt(in.readLine());
        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        ServerItem server = serverList.get(nodeID);
        List<String> tuples = TupleSpace.loadTupleFile(login, name);
        for (String tuple : tuples) {
            String subs[] = tuple.split("&");
            String tupleStr = subs[0].split(":", 3)[2];
            tupleStr = tupleStr.substring(1, tupleStr.length()-1);
            int hashVal = Integer.parseInt(subs[1]);
            String tupleFlag = subs[2];
            if (flag.equals(tupleFlag)) {
                continue;
            }
            int[] ids = ConsistentHash.getIds(hashVal);
            if ((flag.equals("origin") && nodeID == ids[0]) ||
                (flag.equals("backup") && nodeID == ids[1])) {
                Client client = new Client(login, name);
                client.runClient(server.getIP(), server.getPort(), "sendTupleTo " + nodeID + " " + flag + " " + hashVal + " "+ tupleStr);
            }
        }
    }

    /**
     *  return killed file
     */
    private File getKilledFile() {
        String path = "/tmp/" + login + "/linda/" + name + "/killed";
        File killed = new File(path);
        return killed;
    }

    /**
     * run and initiate server, let it listen to host port
     */
    public void run() {
        ServerSocket listener;
        try {
            listener = new ServerSocket(0);
            IP = InetAddress.getLocalHost();
            PORT = listener.getLocalPort();

            init(IP.getHostAddress(), PORT);

            //System.out.println("Server starts...");
            System.out.println(IP.getHostAddress() + " at port number: " + PORT);
            boolean mustRun = true;
            while (mustRun) {
                handler(listener.accept());
            }
            listener.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * deal with request from client, and send request to particular function
     */
    private void handler(Socket clientSocket) {
        try {
                // Create character streams for the socket.
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String input = in.readLine();
                String response = "";
                switch (input) {
                    case "add": addHandler(in);
                                break;
                    case "forward": forwardHandler(in);
                                break;
                    case "out": response = outHandler(in);
                                break;
                    case "in": response = inHandler(in);
                                break;
                    case "rd": response = rdHandler(in);
                                break;
                    case "check": response = checkHandler(in);
                                break;
                    // the removed host transfer their data.
                    case "deleted": transferTupleHandler(in);
                                break;
                    case "cleanUp": cleanUp();
                                break;
                    /*
                     get update request from deleted host to update serverList.
                     the newest serverList is passed in string format.
                      */
                    case "update": updateServerListHandler(in);
                                break;
                    case "redistribute": redistributeHandler(in);
                                break;
                    case "deleteSameTuples": deleteSameTuples(in);
                                break;
                    case "updateTupleAfterAll": updateTupleAfterAllHandler();
                                break;
                    case "getUpdateTuple": getUpdateTupleHandler(in);
                                break;
                    case "recover": recoverHandler(in);
                                break;
                }

                //System.out.println("clientSocket: " + clientStr[0] +" : "+ isequal);
                if (response.length() > 0) {
                    String clientInfo = clientSocket.getRemoteSocketAddress().toString();
                    String[] clientStr = clientInfo.substring(1).split(":");
                    boolean isequal = clientStr[0].equals(IP.getHostAddress());
                    // todo changge to IP equal
                    boolean tmp = clientStr[1].equals(PORT+"");
                    if (!tmp && !input.equals("check")) {
                        System.out.println("Server Received: " + response);
                        System.out.print("linda> ");
                    }
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                    out.println(response);
                    out.close();
                }
            } catch (Exception e) {
                System.out.println("Exception happens when dealing with Server: " + e);
                e.printStackTrace();
            }

            // close clientSocket after finish request.
            try {
                clientSocket.close();
            } catch (Exception e) {
                System.out.println("Exception happens when trying to close Socket: " + e);
                e.printStackTrace();
            }
    }
}