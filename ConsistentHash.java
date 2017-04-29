import java.security.NoSuchAlgorithmException;

/**
 * Created by Ting on 4/24/17.
 */

public class ConsistentHash {

    private static int getSlotNo () {
        int SLOT_NO = 65536; // The total number of slots, which is 2^16
        return SLOT_NO;
    }

    // according to the current number of hosts, update the lookUpTable
    public static int[] updateLookUpTable (int size) {
        int SLOT_NO = getSlotNo();
        int[] lookUpTable = new int[SLOT_NO];
        int slot = SLOT_NO/size; // how many slots one host has
        int count = 0;
        int id = 0;
        for (int i = 0; i < SLOT_NO; i++) {
            lookUpTable[i] = id;
            count++;
            if (id != size-1 && count == slot) {
                id++;
                count = 0;
            }
        }
        return lookUpTable;
    }

    // according to the update table, update the backUpTable
    public static int[] updateBackUpTable (int[] lookUpTable) {
        int SLOT_NO = getSlotNo();
        int[] backUpTable = new int[SLOT_NO];
        for (int i = 0; i < lookUpTable.length; i++) {
            backUpTable[i] = lookUpTable[(i+ SLOT_NO/2)%SLOT_NO];
        }
        return backUpTable;
    }

    // return the original host Id and backUpHost Id of tuple
    public static int[] getIds(int hashVal, int[] lookUpTable, int[] backUpTable) {
        int SLOT_NO = getSlotNo();
        int slotIndex = hashVal%SLOT_NO;
        int[] hostInfo = new int[2];
        hostInfo[0] = lookUpTable[slotIndex];
        hostInfo[1] = backUpTable[slotIndex];
        int temp = slotIndex;
        while (hostInfo[0] == hostInfo[1]) {
            temp++;
            if (temp == SLOT_NO) {
                temp = 0;
            }
            if (temp == slotIndex) {
                break;
            }
            hostInfo[1] = backUpTable[temp];
        }
        return hostInfo;
    }

}
