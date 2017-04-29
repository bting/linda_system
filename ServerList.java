import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
/**
 * Created by Ting on 4/8/17.
 */

public class ServerList {
    /**
     * if server file path does not exist, create it
     * and then return the file path
     */
    private static String getServerFilePath(String login, String name) {
        String path = "/tmp/" + login + "/linda/" + name + "/nets";
        File file = new File(path);
        file.getParentFile().mkdirs();
        changeMode(login, name, "nets");
        return path;
    }
    /**
     * load server-List from disk
     */
    public static List<ServerItem> loadServerList(String login, String name) throws IOException {
        String path = getServerFilePath(login, name);
        BufferedReader inputFile = new BufferedReader(new FileReader(path));
        String input;
        List<ServerItem> serverList = new ArrayList<>();
        while((input = inputFile.readLine()) != null) {
            String[] strs = input.split(" ");
            String hostName = strs[0];
            String IP = strs[1];
            int port = Integer.parseInt(strs[2]);
            ServerItem server = new ServerItem(hostName, IP, port);
            serverList.add(server);
        }
        return serverList;
    }

    /**
     * write server-List into disk
     */
    public static void saveServerList(String login, String name, List<ServerItem> serverList) throws IOException {
        String path = getServerFilePath(login, name);
        BufferedWriter bw = new BufferedWriter(new FileWriter(path));
        for(ServerItem item: serverList) {
            String content = item.getName() + " " + item.getIP() + " " + item.getPort() + "\n";
            bw.write(content);
        }
        bw.close();
    }

    /**
     * return serverList as an entire string
     */
    public static String getServerListContent(String login, String name) throws IOException {
        String path = getServerFilePath(login, name);
        return new String(Files.readAllBytes(Paths.get(path)));
    }

    /**
     * map hostName to serverItem, and return a hashmap of it.
     */
    public static HashMap<String, ServerItem> getHostNameMap(String login, String name, List<ServerItem> servers) throws IOException {
        HashMap<String, ServerItem> hosts = new HashMap<>();
        for (int i = 0; i < servers.size(); i++) {
            String hName = servers.get(i).getName();
            hosts.put(hName, servers.get(i));
        }
        return hosts;
    }

    public static void writeServer(String login, String name, ServerItem server) throws IOException {
        String path = getServerFilePath(login, name);
        BufferedWriter bw = new BufferedWriter(new FileWriter(path));
        String content = server.getIP() + " " + server.getPort();
        bw.write(content);
    }

    public static void changeMode(String login, String name, String fileName) {
        File file = new File("/tmp/" + login + "/linda");
        file.setExecutable(true, false);
        file.setWritable(true, false);
        file.setReadable(true, false);

        File file2 = new File("/tmp/" + login +"/linda/" + name);
        file2.setExecutable(true, false);
        file2.setWritable(true, false);
        file2.setReadable(true, false);

        File file3 = new File("/tmp/" + login +"/linda/" + name + "/" + fileName);
        file3.setExecutable(false, false);
        file3.setWritable(true, false);
        file3.setReadable(true, false);
    }
}
