import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.PriorityQueue;

public class DataBaseEngine {
    private final String dataBaseLogFilePath;
    private HashMap<String, String> keyValueStore;

    private static final byte OP_PUT = 0;
    private static final byte OP_DEL = 1;

    public DataBaseEngine(String databaseLogFilePath){
        this.dataBaseLogFilePath = databaseLogFilePath;
        keyValueStore = new HashMap<>();
        startup();
    }

    private void startup(){
        //start up the database engine
        //build the up to date state from log file
        //check if log file exists else create one
        File file = new File(dataBaseLogFilePath);
        if(!file.exists()){
            try {
                System.out.println(getClass().toString() + "startup() -> File did not exist, creating new file");
                file.createNewFile();
            } catch (IOException e) {
                System.out.println(getClass().toString() + "startup() -> Exception occured " + e.getMessage());
                throw new RuntimeException(e);
            }
        }
        try(DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))){
            while(true){
                try{
                    byte opByte = in.readByte();

                    if(opByte == OP_DEL){
                        //opType is DEL
                        int keyBytesLen = in.readInt();
                        byte[] keyBytes = in.readNBytes(keyBytesLen);
                        //now if we have read till here then this is a valid record and we can update the hashmap
                        String key = CommonUtils.getString(keyBytes);
                        keyValueStore.remove(key);
                    }else if(opByte == OP_PUT){
                        //opType is PUT
                        int keyBytesLen = in.readInt();
                        byte[] keyBytes = in.readNBytes(keyBytesLen);
                        int valBytesLen = in.readInt();
                        byte[] valBytes = in.readNBytes(valBytesLen);
                        //now if we have read till here then this is a valid record and we can update the hashmap
                        String key = CommonUtils.getString(keyBytes);
                        String val = CommonUtils.getString(valBytes);
                        keyValueStore.put(key, val);
                    }else{
                        throw new RuntimeException("Unrecognized opType for opType byte " + opByte);
                    }

                }catch(EOFException e){
                    System.out.println("startup()-> Reached EOF");
                    break;
                }
            }

        }catch(Exception e){
            throw new RuntimeException("Failed to start the data store ", e);
        }
    }

    public String set(String key, String value){
        try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataBaseLogFilePath, true)))){
            out.writeByte(OP_PUT);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            out.writeInt(keyBytes.length);
            out.write(keyBytes);

            byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeInt(valBytes.length);
            out.write(valBytes);

        }catch(IOException e){
            throw new RuntimeException("set()->Failed to write to log ", e);
        }
        return keyValueStore.put(key, value);
    }

    public String del(String key){
        try(DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataBaseLogFilePath)))){
            out.writeByte(OP_DEL);
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
        }catch (IOException e){
            throw new RuntimeException("del()->Failed to write to log ", e);
        }
        return keyValueStore.remove(key);
    }

    public String get(String key){
        return keyValueStore.get(key);
    }
}
