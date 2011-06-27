import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InsertSamples {
    public static void main(String[] args) {
        InsertSamples instance = new InsertSamples();
        try {
            instance.run();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private void run() throws UnknownHostException, InterruptedException {
        Mongo mongo = new Mongo("127.0.0.1", 27017);
        long l = System.currentTimeMillis();

        mongo.getDB("test").getCollection("samples").drop();

        Set<Thread> list = new HashSet<Thread>(10);
        int numClients = 1;
        int documentsPerClient = 1000000;

        System.out.println("starting " + numClients + " clients, each of which shall insert " + documentsPerClient + " documents");

        for (int i = 0; i < numClients; i++) {
            InsertThread t = new InsertThread("thread" + i, mongo.getDB("test"), numClients, documentsPerClient);
            t.start();
            list.add(t);
        }
    }

    private class InsertThread extends Thread {
        private final String threadName;
        private final DB db;
        private final int numClients;
        private final int documentsPerClient;

        public InsertThread(String threadName, DB db, int numClients, int documentsPerClient) {
            this.threadName = threadName;
            this.db = db;
            this.numClients = numClients;
            this.documentsPerClient = documentsPerClient;
        }

        @Override
        public void run() {
            System.out.println("start client " + threadName);
            long t = System.currentTimeMillis();
            DBCollection samples = db.getCollection("samples");
            int bulkSize = 1000;
            List<DBObject> list = new ArrayList<DBObject>(bulkSize);
            for (int i = 0; i < documentsPerClient; i++) {
                list.add(BasicDBObjectBuilder.start("client", this.threadName).add("docNum", i).add("time", new Date()).get());
                if (i >= (bulkSize - 1)) {
                    samples.insert(list, WriteConcern.NORMAL);
                    list.clear();
                    //System.out.println(threadName+" : flushed");
                }
            }

            float time = (System.currentTimeMillis() - t) / 1000f;
            System.out.println(threadName + " finished after " + time + "secs. given this is the last thread to finish, that means " + ((documentsPerClient * numClients) / time) + " inserts per second.");
        }
    }
}
