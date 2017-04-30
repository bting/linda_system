/**
 * Created by Ting on 4/8/17.
 */

/**
 * define ServerItem, and store information of server
 * Like hostName, IP address and port number
 */
public class ServerItem {
    private String hostName;
    private String IP;
    private int port;
    public ServerItem(String h, String i, int p) {
        hostName = h;
        IP = i;
        port = p;
    }

    /**
     * @return IP address of server
     */
    public String getIP() {
        return IP;
    }

    /**
     * @return port number of server
     */
    public int getPort() {
        return port;
    }

    /**
     * @return host name of server
     */
    public String getName() {
        return hostName;
    }

    /**
     * set port number
     */
    public void setPort(int p) {
        port = p;
    }
}
