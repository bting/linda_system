import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Ting on 4/14/17.
 */
public class ErrorCheck {
    /**
     * check whether the add command is valid or not
     */
    public static boolean addRequestCheck(String subcommand) {
        Matcher m = Pattern.compile("\\(([^)]+)\\)").matcher(subcommand);
        ArrayList<String> strs = new ArrayList<>();
        while (m.find()) {
            strs.add(m.group(1));
            //System.out.println(m.group(1));
        }
        if (strs.size() == 0) {
            System.out.println("please add correct host information");
            return false;
        }
        for (int i = 0; i < strs.size(); i++) {
            String server = strs.get(i).trim();
            if(!isValServer(server)) {
                return false;
            }
        }
        return true;
    }

    public static boolean deleteRequestCheck(String subcommand) {
        Matcher m = Pattern.compile("\\(([^)]+)\\)").matcher(subcommand);
        ArrayList<String> strs = new ArrayList<>();
        while (m.find()) {
            strs.add(m.group(1));
            //System.out.println(m.group(1));
        }
        if (strs.size() == 0) {
            System.out.println("please input correct host information");
            return false;
        }
        return true;
    }

    /**
     * check whether the information of added_host is valid or not
     * like Ip address, port number and so on
     */
    public static boolean isValServer(String server) {
        String[] serverIfo = server.trim().split(",");
        if (serverIfo.length != 3) {
            System.out.println("please input correct host information");
            return false;
        }
        if (!serverIfo[0].trim().startsWith("host_")) {
            System.out.println("please input correct host_name, must start with \"host_\"");
            return false;
        }
        String[] Ips = serverIfo[1].trim().split("\\.");
        if (Ips.length != 4) {
            System.out.println("please input correct IP address");
            return false;
        }
        for (int i = 0; i < Ips.length; i++) {
            if(Ips[i].length() < 1 || !Character.isDigit(Ips[i].charAt(0))) {
                System.out.println("please input correct IP address");
                return false;
            }
            int num = Integer.parseInt(Ips[i].trim());
            if (num < 0 || num > 255) {
                System.out.println("please input correct IP address");
                return false;
            }
        }

        int port = Integer.parseInt(serverIfo[2].trim());
        if (port < 1024) {
            System.out.println("please input correct port number");
            return false;
        }
        return true;
    }


    /**
     * check whether the add/rd/out request is correct or not
     */
    public static boolean tupleCheck(String subcommand, boolean isOut) {
        Matcher m = Pattern.compile("\\(([^)]+)\\)").matcher(subcommand);
        ArrayList<String> strs = new ArrayList<>();
        while (m.find()) {
            strs.add(m.group(1));
            //System.out.println(m.group(1));
        }
        if (strs.size() == 0) {
            System.out.println("please input correct out request");
            return false;
        }
        for (int i = 0; i < strs.size(); i++) {
            String s = strs.get(i).trim();
            if (!isValTuples(s, isOut)) {
                return false;
            }
        }
        return true;
    }

    /**
     * check whether the input tuple is valid or not
     */
    public static boolean isValTuples(String tuple, boolean isOut) {
        String[] tuples = tuple.split(",");
        if (tuples.length == 0) {
            System.out.println("please input valid tuple");
            return false;
        }
        for (int i = 0; i < tuples.length; i++) {
            String t = tuples[i].trim();
            if (t.startsWith("\"")) {
                if (t.charAt(t.length()-1) != '\"') {
                    System.out.println("please input correct string");
                    return false;
                }
            } else if (!isOut && t.startsWith("?")) {
                if (t.charAt(1) == 'i') {
                    if (!t.substring(3).equals("int")) {
                        System.out.println("please input correct int format: ?i:int");
                        return false;
                    }
                } else if (t.charAt(1) == 'f') {
                    if (!t.substring(3).equals("float")) {
                        System.out.println("please input correct float format: ?f:float");
                        return false;
                    }
                } else if (t.charAt(1) == 's') {
                    if (!t.substring(3).equals("string")) {
                        System.out.println("please input correct string format: ?s:string");
                        return false;
                    }
                } else {
                    System.out.println("Please input correct wild format: ?s:string, ?i:int or ?f:float");
                    return false;
                }

            } else {
                int count = 0;
                if (t.length() < 1 || !Character.isDigit(t.charAt(0))) {
                    System.out.println("please input correct number");
                    return false;
                }
                for (int j = 1; j < t.length(); j++) {
                    if (t.charAt(j) == '.') {
                        count++;
                    } else if (count > 1 || !Character.isDigit(t.charAt(j))) {
                        System.out.println("please input correct number");
                        return false;
                    }
                }
            }
        }
        return true;
    }

}
