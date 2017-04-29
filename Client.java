import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Client {
    String login;
    String name;
    String IP;
    int serverPort;
    boolean isMaster = false;

    public Client(String l, String n) {
        login = l;
        name = n;
    }

    /**
     * broadcast message to all connected hosts
     * deal with rd & in command
     */
    private String broadcastMessage(String command, String isVariable, String tupleStr) throws IOException, InterruptedException {
        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        while (true) {
            for (ServerItem server : serverList) {
                Socket skt = new Socket(server.getIP(), server.getPort());
                PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
                if (command.equals("in")) {

                }
                out.println(command);
                out.println(isVariable);
                out.println(tupleStr);
                BufferedReader in = new BufferedReader(new InputStreamReader(skt.getInputStream()));
                String input = in.readLine();
                if (input != null && input.length() > 0) {
                    return input;
                }
                out.close();
            }
            Thread.sleep(1000);
        }
    }


    /**
     * send message to particular host (this host get from hash-function)
     * deal with rd & in command
     */
    private String sendMessage(String command, String isVariable, String tupleStr, int hashVal) throws IOException, InterruptedException {
        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        int[] lookUpTable = ConsistentHash.updateLookUpTable(serverList.size());
        int[] backUpTable = ConsistentHash.updateBackUpTable(lookUpTable);
        int[] ids = ConsistentHash.getIds(hashVal, lookUpTable, backUpTable);
        String response = "";

        while (true) {
            for (int i = 0; i < ids.length; i++) {
                ServerItem server = serverList.get(ids[i]);
                Socket skt = new Socket(server.getIP(), server.getPort());
                PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
                out.println(command);
                out.println(isVariable);
                out.println(tupleStr);
                BufferedReader in = new BufferedReader(new InputStreamReader(skt.getInputStream()));
                String input = in.readLine();
                if (input != null && input.length() > 0) {
                    if (response.length() == 0) {
                        response = input;
                    }
                }
                out.close();
            }
            if (response.length() > 0) {
                return response;
            }
            Thread.sleep(1000);
        }
    }


    /**
     * add remoteServer's IP and port into its own serverList
     * the format is add (host_1, 129.210.16.80, 9998) (host_2, 129.210.16.80, 5678)
     * need to check the input add command is inputted on the new host or the original hosts.
     */
    private void addRequest(String subCommand) throws IOException {
        Matcher m = Pattern.compile("\\(([^)]+)\\)").matcher(subCommand);
        ArrayList<String> strs = new ArrayList<>();
        while (m.find()) {
            strs.add(m.group(1));
            //System.out.println(m.group(1));
        }
        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        HashMap<String, ServerItem> hostMap = ServerList.getHostNameMap(login, name, serverList);
        // check whether the host which get input request is master or not.
        isMaster = (serverList.size() > 1);
        ServerItem master = new ServerItem(name, IP, serverPort);

        // put all the hosts after "add" command into serverList.
        for (int i = 0; i < strs.size(); i++) {
            String[] newServer = strs.get(i).split(",");
            String hostName = newServer[0].trim();
            String ip = newServer[1].trim();
            int port = Integer.parseInt(newServer[2].trim());
            if (hostMap.containsKey(hostName)) {
                ServerItem temp = hostMap.get(hostName);
                if (temp.getIP().equals(ip) && temp.getPort() == port) {
                    System.out.println("Failed: " + hostName + "has already existed.");
                    return;
                }
                System.out.println("Failed: Have duplicate host name: " + hostName);
                return;
            }
            ServerItem s = new ServerItem(hostName, ip, port);
            hostMap.put(hostName, s);
            serverList.add(s);
        }

        /*
         if the host which get input request is not master. check whether the added hosts are master or not.
         if has, choose anyone of the masters. if not, the current host is the master.
         */
        if(!isMaster) {
            for (int j = 0; j < serverList.size(); j++) {
                ServerItem tempItem = serverList.get(j);
                Socket skt = new Socket(tempItem.getIP(), tempItem.getPort());
                //System.out.println(tempItem.getIP() + " : " + tempItem.getPort());
                PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
                out.println("check");
                BufferedReader in = new BufferedReader(new InputStreamReader(skt.getInputStream()));
                String input = in.readLine();
                if (input.equals("true")) {
                    master = tempItem;
                    break;
                }
                out.close();
            }
            /*
             find one master, and ask it to add all the newest hosts,
             and then let it to inform all the hosts in the serverList to update their serverList.
              */
            if (!master.getName().equals(name)) {
                passAddRequest(serverList, master);
                return;
            }
        }
        ServerList.saveServerList(login, name, serverList);
        updateServerList(serverList);
    }

    /**
     * ask one of the master to add all the newComer's IP and port into old grouped serverList
     * and pass the newAdded hosts' into string format.
     */
    private void passAddRequest(List<ServerItem> serverList, ServerItem master) throws IOException {
        Socket skt = new Socket(master.getIP(), master.getPort());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < serverList.size(); i++) {
            ServerItem temp = serverList.get(i);
            if (temp.getName().equals(master.getName())) {
                continue;
            }
            sb.append(temp.getName() + " " + temp.getIP() + " " + temp.getPort() + " ");
        }
        String send = sb.toString();
        PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
        out.println("add");
        out.println(send.trim());
        out.close();
    }

    /**
     * remove host from serverList.
     * also need to check the delete command is inputted in the deleted host or in the left hosts.
     * the format is delete (host_1, host_2, host_3)
     * After all the tuples has been transfered of removed host,
     * ask all the host of the newest list to update their tuple list.
     */
    private void deleteHostRequest(String subCommand) throws IOException {
        Matcher m = Pattern.compile("\\(([^)]+)\\)").matcher(subCommand);
        ArrayList<String> strs = new ArrayList<>();
        ArrayList<ServerItem> removedList = new ArrayList<>();
        while (m.find()) {
            strs.add(m.group(1));
            //System.out.println(m.group(1));
        }
        if (strs.size() != 1) {
            System.out.println("Please input correct delete host request.");
        }

        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        HashMap<String, ServerItem> hostMap = ServerList.getHostNameMap(login, name, serverList);
        String[] hostList = strs.get(0).split(",");

        // check whether hostName exist or not, and remove it from hostMap
        for (String host : hostList) {
            String tempHost = host.trim();
            if (!hostMap.containsKey(tempHost)) {
                System.out.println("Host " + tempHost + "does not exist.");
                return;
            }
            removedList.add(hostMap.get(tempHost));
            hostMap.remove(tempHost);
        }

        // create new list to store the left hosts
        List<ServerItem> newList = new ArrayList<>();
        for(ServerItem server : serverList) {
            String tempName = server.getName();
            if (hostMap.containsKey(tempName)) {
                newList.add(server);
            }
        }

        // pass the left hosts into string and pass it to removed hosts to redistribute data to them
        StringBuilder sb = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for (ServerItem server : newList) {
            sb.append(server.getName()).append(" ");
            sb2.append(server.getName()).append(" ").append(server.getIP()).append(" ").append(server.getPort()).append(" ");
        }
        String updateInfo = sb2.toString();
        String passServerInfo = sb.toString();

        // if hostMap contains current host's name, it means current host would be left.
        // then directly update its serverList and inform others in the newList to update their serverList too.
        if (hostMap.containsKey(name)) {
            ServerList.saveServerList(login, name, newList);
            updateServerList(newList);
        } else {
            // if host call delete request to delete itself
            // then you need to inform all the remaining hosts to update their serverList.
            for (ServerItem item: newList) {
                Socket skt = new Socket(item.getIP(), item.getPort());
                PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
                out.println("update");
                out.println(updateInfo.trim());
                out.close();
            }
        }

        // connect to all removed hosts to delete and transfer their data
        // and pass the left servers info to every removed host, let them know where to transfer their data.
        for (ServerItem toRemove : removedList) {
            Socket skt = new Socket(toRemove.getIP(), toRemove.getPort());
            PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
            out.println("deleted");
            out.println(passServerInfo.trim());
            out.close();
        }
    }

    /**
     * pass the latest serverList as parameter
     * inform every hosts in the new serverList to update their tuple List
     * @throws IOException
     */
    private void updateTupeAfterAll(List<ServerItem> serverList) throws IOException {
        for (ServerItem item: serverList) {
            Socket skt = new Socket(item.getIP(), item.getPort());
            PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
            out.println("updateTupleAfterAll");
            out.close();
        }
    }

    /**
     * the current host is in the latest serverList
     * broadcast all the server in host's serverList to update their serverList
     */
    private void updateServerList(List<ServerItem> serverList) throws IOException {
        String serverListContent = ServerList.getServerListContent(login, name);
        //List<ServerItem> serverList = ServerList.loadServerList(login, name);
        for (ServerItem server: serverList) {
            if (server.getIP().equals(IP) && server.getPort() == serverPort) {
                continue;
            }
            Socket skt = new Socket(server.getIP(), server.getPort());
            PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
            out.println("forward");
            out.println(serverListContent);
            out.close();
        }
        // once update serverList, need to update tuples of the newServerList too.
        updateTupeAfterAll(serverList);
    }

    /**
     * check whether the sub-command has variables
     */
    private boolean hasVariable(String[] strs) {
        boolean hasVal = false;
        for (int i = 0; i < strs.length; i++) {
            String s = strs[i].trim();
            if (s.startsWith("?")) {
                hasVal = true;
                break;
            }
        }
        return hasVal;
    }


    /**
     * deal with "in" request, for example:
     * in("abc", 3)
     * in("abc", ?i:int)
     */
    private void inRequest(String subCommand) throws IOException, NoSuchAlgorithmException, InterruptedException {
        String temp = subCommand.substring(2).trim();
        String tupleStr = temp.substring(1, temp.length()-1).trim();
        String[] strs = tupleStr.split(",");
        String response;
        if (hasVariable(strs)) {
            response = broadcastMessage("in", "true", tupleStr);
        } else {
            int hashVal = Md5Sum.getHashVal(tupleStr);
            response = sendMessage("in", "false", tupleStr, hashVal);
        }
        System.out.println(response);
    }

    /**
     * deal with "rd" request, for example:
     * rd("abc", 3)
     * rd("abc", ?i:int)
     */
    private void rdRequest(String subCommand) throws IOException, NoSuchAlgorithmException, InterruptedException {
        String temp = subCommand.substring(2).trim();
        String tupleStr = temp.substring(1, temp.length()-1).trim();
        String[] strs = tupleStr.split(",");
        String response;
        if (hasVariable(strs)) {
            response = broadcastMessage("rd", "true", tupleStr);
        } else {
            int hashVal = Md5Sum.getHashVal(tupleStr);
            response = sendMessage("rd", "false", tupleStr, hashVal);
        }
        System.out.println(response);
    }

    /**
     * deal with "out" request, for example:
     * out("abc", 3)
     */
    private void outRequest(String subCommand) throws IOException, InterruptedException, NoSuchAlgorithmException {
        // out(“abc”, 3)
        String temp = subCommand.substring(3).trim();
        String tupleStr = temp.substring(1, temp.length()-1).trim();
        int hashVal = Md5Sum.getHashVal(tupleStr);
        //int serverID = getHashID(tupleStr);

        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        storeTuples("out", tupleStr, hashVal, serverList);
    }

    /**
     * 1.according to the oldserverList, oldlookUpTable, oldBackUpTable
     *   remove all the same tuples(which is same to removed host) of all server hosts
     * 2.move all the tuples of the removed host to the last_left hosts
     * 3.after all the tuple of removed host have been redistributed, tell removed host to reinitiate itself.
     * 4.inform all the hosts in the newest serverlist to update their tuple list finally
     */
    private void RemoveTupleRequest(String subCommand) throws IOException {
        String[] commands = subCommand.split("&");
        if (commands.length != 2) {
            System.out.println("Error happens when try to move tuples.");
            return;
        }
        String[] hosts = commands[1].split(" ");
        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        // oldlookUpTable & oldBackUpTable used for removeSameTuple
        int[] oldlookUpTable = ConsistentHash.updateLookUpTable(serverList.size());
        int[] oldBackUpTable = ConsistentHash.updateBackUpTable(oldlookUpTable);

        HashMap<String, ServerItem> hostMap = ServerList.getHostNameMap(login, name, serverList);
        List<ServerItem> newServerList = new ArrayList<>();
        for (String host : hosts) {
            String hName = host.trim();
            if (!hostMap.containsKey(hName)) {
                System.out.println("Error happens when try to move tuples. Host " + hName + " can't be found." );
                return;
            }
            newServerList.add(hostMap.get(hName));
        }

        List<String> tuples = TupleSpace.loadTupleFile(login, name);

        // Remove all the same tuples(which is same to removed host) of all server hosts
        // according to the oldserverList, oldlookUpTable, oldBackUpTable
        removeSameTuple(serverList, oldlookUpTable, oldBackUpTable, tuples);
        moveTuples(newServerList, tuples);
        //after all the tuple of removed host have been redistributed, tell removed host to re-initiate itself.
        Socket skt = new Socket(IP, serverPort);
        PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
        out.println("cleanUp");
        out.close();

        /*
        inform all the hosts in the newest serverlist to update their tuple list
         */
        //updateTupeAfterAll(newServerList);
    }

    /**
     * deal with out & redistribute command.
     * store tuples into original host & backup host.
     */
    private void storeTuples(String command, String tupleStr, int hashVal, List<ServerItem> newServerList) {
        int[] lookUpTable = ConsistentHash.updateLookUpTable(newServerList.size());
        int[] backUpTable = ConsistentHash.updateBackUpTable(lookUpTable);
        int[] ids = ConsistentHash.getIds(hashVal, lookUpTable, backUpTable);
        String response;
        for (int i = 0; i < ids.length; i++) {
            ServerItem server = newServerList.get(ids[i]);
            try {
                Socket skt = new Socket(server.getIP(), server.getPort());
                PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(skt.getInputStream()));
                out.println(command);
                out.println(tupleStr);
                out.println(hashVal+"");
                if (i == 0) {
                    out.println("origin");
                } else {
                    out.println("backup");
                }
                response = in.readLine();
                if (response != null && response.length() > 0) {
                    System.out.println(response);
                }
                out.close();
            } catch (java.net.ConnectException e) {
                System.out.println(server.getName() + " is disconnected.");
                System.out.println("(" + tupleStr + ")" + "has been put on its backup host: " + newServerList.get(ids[(i+1)%2]).getName());
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * remove all the tuples, which is the same in removed hosts, from all the original hosts.
     */
    private void removeSameTuple(List<ServerItem> oldserverList, int[] oldlookUpTable, int[] oldBackUpTable, List<String> tuples ) throws IOException {
        for (String tuple : tuples) {
            String[] tupleStr = tuple.split("&");
            int hashVal = Integer.parseInt(tupleStr[1]);
            int[] ids = ConsistentHash.getIds(hashVal, oldlookUpTable, oldBackUpTable);
            for (int j = 0; j < ids.length; j++) {
                ServerItem server = oldserverList.get(ids[j]);// back up host
                Socket skt = new Socket(server.getIP(), server.getPort());
                PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
                out.println("deleteSameTuples");
                out.println(tupleStr[0] + "&" + tupleStr[1]);
                if (j == 0) {
                    out.println("origin");
                } else {
                    out.println("backup");
                }
            }
        }
    }

    /**
     * which is called in RemoveTupleRequest function.
     * according to hashVal of each tuple of removed hosts,
     * redistribute tuples to the particular left hosts (store in origin, backup these two format)
     */
    private void moveTuples(List<ServerItem> newServerList, List<String> tuples ) throws IOException {
        for (String tuple : tuples) {
            String[] tupleSplit = tuple.split("&");
            String tupleStr = tupleSplit[0];
            int hashVal = Integer.parseInt(tupleSplit[1]);
            storeTuples("redistribute", tupleStr, hashVal, newServerList);
        }
    }

    /**
     * be called after add & delete request.
     * subCommand already include list of tuples which need to be updated.
     * ask all the tuples which need to be updated to store in their new particular host.
     */
    private void moveTuplesAfterAll(String subCommand) throws IOException {
        List<ServerItem> serverList = ServerList.loadServerList(login, name);
        int[] lookupTable = ConsistentHash.updateLookUpTable(serverList.size());
        int[] backupTable = ConsistentHash.updateBackUpTable(lookupTable);
        String[] subs = subCommand.trim().split(" ");
        for (int i = 1; i < subs.length; i++) {
            String tuple = subs[i].trim();
            String[] tupleInfo = tuple.split("&");
            int hashVal = Integer.parseInt(tupleInfo[1]);
            int[] ids = ConsistentHash.getIds(hashVal, lookupTable, backupTable);
            ServerItem server;
            if (tupleInfo[2].equals("backup")) {
                server = serverList.get(ids[1]);
            } else {
                server = serverList.get(ids[0]);
            }
            Socket skt = new Socket(server.getIP(), server.getPort());
            PrintWriter out = new PrintWriter(skt.getOutputStream(), true);
            out.println("getUpdateTuple");
            out.println(tuple);
            out.close();
        }
    }

    /**
     * Runs the client as an application with a closeable frame.
     */
    public void runClient(String ip, int port, String subCommand) throws Exception {
        // save host server's ip address and port number
        IP = ip;
        serverPort = port;

        /*
         * check whether command is valid or not
         * and then hand to particular function according to different request.
         */
        subCommand = subCommand.trim();
        if (subCommand.length() != 0) {
            if (subCommand.startsWith("add") && ErrorCheck.addRequestCheck(subCommand)) {
                addRequest(subCommand);
            } else if (subCommand.startsWith("in") && ErrorCheck.tupleCheck(subCommand, false)) {
                inRequest(subCommand);
            } else if (subCommand.startsWith("rd") && ErrorCheck.tupleCheck(subCommand, false)) {
                rdRequest(subCommand);
            } else if (subCommand.startsWith("out") && ErrorCheck.tupleCheck(subCommand, true)) {
                outRequest(subCommand);
            } else if (subCommand.startsWith("forward")) {
                List<ServerItem> serverList = ServerList.loadServerList(login, name);
                updateServerList(serverList);
            } else if (subCommand.startsWith("delete")) {
                deleteHostRequest(subCommand);
            } else if (subCommand.startsWith("remove")) {
                RemoveTupleRequest(subCommand);
            } else if (subCommand.startsWith("moveTupleAfterAll")) {
                moveTuplesAfterAll(subCommand);
            } else {
                System.out.println("Command: " + subCommand + " does not exist");
                //System.out.print("linda> ");
            }
        }
    }
}