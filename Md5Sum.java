import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/**
 * Created by Ting on 4/24/17.
 */
public class Md5Sum {
    public static int getHashVal(String target) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update((target.getBytes()));
        byte[] digest = md.digest();
        StringBuilder sb = new StringBuilder();
        for(byte b: digest) {
            sb.append(String.format("%02x", b & 0xff));
        }

        String str = sb.toString();
        String digits = "0123456789ABCDEF";
        str = str.toUpperCase();
        int val = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            int digit = digits.indexOf(c);
            val = 16*val + digit;
        }
        if (val < 0) {
            val = -val;
        }
        return val;
    }
}
