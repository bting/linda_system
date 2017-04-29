import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ting on 4/9/17.
 */
public class TupleSpace {
    /**
     * If filePath not exist, create filePath
     * return filePath
     */
    private  static String getFilePath(String login, String name) {
        String path = "/tmp/" + login + "/linda/" + name + "/tuples";
        File file = new File(path);
        file.getParentFile().mkdirs();
        ServerList.changeMode(login, name, "tuples");
        return path;
    }

    public  static void removeTupleFile(String login, String name) {
        File tupleFile = new File(getFilePath(login, name));
        tupleFile.delete();
    }

    /**
     * load tuples from disk into memory
     * @param login, name
     * @return List of tuple, tuple represent as JSONArray
     * @throws IOException, ParseException
     */
    public static List<String> loadTupleFile(String login, String name) throws IOException {
        String path = getFilePath(login, name);
        BufferedReader inputFile = new BufferedReader(new FileReader(path));
        List<String> tuples = new ArrayList<>();
        String line;
        while ((line = inputFile.readLine()) != null) {
            tuples.add(line);
        }
        return tuples;
    }

    /**
     * save tuples from memory into disk
     * @param tuples, login, name
     * @throws IOException
     */

    public static void saveTupleFile(List<String> tuples, String login, String name) throws IOException {
        String path = getFilePath(login, name);
        BufferedWriter bw = new BufferedWriter(new FileWriter(path));
        for(String tuple: tuples) {
            bw.write(tuple+"\n");
        }
        bw.close();
    }

    /**
     * save single tuple into disk
     * @param tuple, login, name
     * @throws IOException
     */
    public static void appendToTupleFile(String tuple, String login, String name) throws IOException {
        String path = getFilePath(login, name);
        BufferedWriter bw = new BufferedWriter(new FileWriter(path, true));
        //System.out.println(tuple.toString() + "saved!");
        bw.write(tuple+"\n");
        bw.close();
    }

    /**
     * create original string to tuple format
     * like "abc", 3 ---> 2:si:("abc" 3)
     */
    public static String createTupleFromString(String s, String hashVal, String flag){
        String formatedStr = createMatchedTupleFromString(s);
        formatedStr = formatedStr + "&" + hashVal + "&" + flag;
        return formatedStr;
    }

    /**
     * change simple String into formated tuple which without hashVal & flag(origin, backup)
     * @param s
     * @return
     */
    public static String createMatchedTupleFromString(String s) {
        String[] strs = s.split(",");
        StringBuilder sb = new StringBuilder();
        int size = strs.length;

        String type = getTypesOfTuple(strs);
        sb.append(size+":"+type+":");
        StringBuilder num = new StringBuilder();

        for(int i = 0; i < strs.length; i++) {
            String t = strs[i].trim();
            num.append(t);
            if (i != strs.length-1) {
                num.append(",");
            }
        }
        sb.append("(" + num.toString().trim()+")");
        return sb.toString();
    }

    public static String getTypesOfTuple(String[] strs) {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < strs.length; i++) {
            String t = strs[i].trim();
            int count = 0;
            if (t.startsWith("\"")) {
                sb.append("s");
            } else if (t.startsWith("?")){
               sb.append(t.charAt(1)+"");
            } else if(Character.isDigit(t.charAt(0))) {
                for (int j = 0; j < t.length(); j++) {
                    if (t.charAt(j) == '.') {
                        count++;
                    }
                }
                if (count == 1) {
                    sb.append("f");
                } else {
                    sb.append("i");
                }
            }
        }
        return sb.toString();
    }
}
