import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Ting on 4/24/17.
 */

public class ConsistentHash {
    private static int SLOT_NO = 65535;
    private static int[] lookUpTable = null;
    private static int[] backUpTable = null;

    // update both lookupTable and backupTable
    public static void updateTables(int size) {
        updateLookUpTable(size);
        updateBackUpTable();
    }

    // according to the current number of hosts, update the lookUpTable
    public static void updateLookUpTable (int size) {
        lookUpTable = new int[SLOT_NO];
        int slot = SLOT_NO/size; // how many slots one host has
        int count = 0;
        int id = 0;
        for (int i = 0; i < SLOT_NO; i++) {
            lookUpTable[i] = id;
            count++;
            if (id != size - 1 && count == slot) {
                id++;
                count = 0;
            }
        }
    }

    public static int[] getLookUpTable() {
        return lookUpTable;
    }

    // according to the update table, update the backUpTable
    public static void updateBackUpTable () {
        backUpTable = new int[SLOT_NO];
        for (int i = 0; i < lookUpTable.length; i++) {
            backUpTable[i] = lookUpTable[(i+ SLOT_NO/2)%SLOT_NO];
        }
    }

    public static int[] getBackUpTable() {
        return backUpTable;
    }

    // return the original host Id and backUpHost Id of tuple
    public static int[] getIds(int hashVal) {
        int slotIndex = hashVal%SLOT_NO;
        int[] hostInfo = new int[2];
        hostInfo[0] = lookUpTable[slotIndex];
        hostInfo[1] = backUpTable[slotIndex];
        assert hostInfo[0] != hostInfo[1];
        return hostInfo;
    }

    // return primary nodes of a backup node
    public static Set<Integer> getPrimaryNodes(int backupNodeID) {
        Set<Integer> primaryNodes = new HashSet<>();
        for (int i = 0; i < SLOT_NO; i++) {
            int[] hostInfo = getIds(i);
            if (hostInfo[1] == backupNodeID) {
                primaryNodes.add(hostInfo[0]);
            }
        }
        return primaryNodes;
    }

    // return backup nodes of a primary node
    public static Set<Integer> getBackupNodes(int primaryNodeID) {
        Set<Integer> backupNodes = new HashSet<>();
        for (int i = 0; i < SLOT_NO; i++) {
            int[] hostInfo = getIds(i);
            if (hostInfo[0] == primaryNodeID) {
                backupNodes.add(hostInfo[1]);
            }
        }
        return backupNodes;
    }
}
