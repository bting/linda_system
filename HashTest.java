/**
 * Created by Ting on 4/24/17.
         */
public class HashTest {
    public static void main(String args[]) {
        //System.out.println(Math.pow(2,2));
        int SLOT_NO = 12;
        int[] lookUpTable = new int[SLOT_NO];
        int slot = SLOT_NO/3; // how many slots one host has
        int count = 0;
        int id = 0;
        for (int i = 0; i < SLOT_NO; i++) {
            lookUpTable[i] = id;
            System.out.println(i + ": " + id);
            count++;
            if (count == slot) {
                id++;
                count = 0;
            }
        }

        for (int i = 0; i < lookUpTable.length; i++) {
            int temp = (lookUpTable[i] + SLOT_NO/2)%SLOT_NO;
            System.out.println("backUp: " + i + ": " + temp);
        }

    }
}
