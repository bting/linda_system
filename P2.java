import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Created by Ting on 4/5/17.
 */
public class P2 {
    /**
     * global variables
     */
    private String IP;
    private int PORT;
    private String login = "tbao";
    private String name;

    /**
     * get login name from terminal and run program P1
     * @param args
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
        P2 p2 = new P2();
        p2.name = args[0];
        p2.run();
    }

    /**
     * run server first and then run client
     */
    public void run() {
        try {
            // start server
            Server server = new Server(login, name);
            server.start();
            // let main thread wait for server start
            Thread.sleep(1000);
            // get IP address and port number from server
            String serverInfo = server.getServerInfo();
            String[] strs = serverInfo.split(" ");
            IP = strs[0];
            PORT = Integer.parseInt(strs[1]);

            /* use while loop to continue asking user to input command
             * and for each command start a client to deal with it
             */
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                System.out.print("linda> ");
                String subCommand = reader.readLine();
                if (subCommand == null) {
                    continue;
                }
                Client client = new Client(login, name);
                client.runClient(IP, PORT, subCommand);
            }
        } catch (Exception e) {
            System.out.println("Error happens when run server and client: " + e);
            e.printStackTrace();
        }
    }
}
