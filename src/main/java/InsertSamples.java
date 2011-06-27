import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.*;

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

        int totalDocs = 1000000;
        int numClients = 8;
        int documentsPerClient = totalDocs / numClients;


        Set<Thread> set = new HashSet<Thread>(numClients);


        System.out.println("creating " + numClients + " clients, each of which shall insert " + documentsPerClient + " documents");

        for (int i = 0; i < numClients; i++) {
            InsertThread t = new InsertThread("thread" + i, mongo.getDB("test"), numClients, documentsPerClient);
            set.add(t);
        }

        for (Thread thread : set) {
            thread.start();
        }
    }

    private class InsertThread extends Thread {
        private final String threadName;
        private final DB db;
        private final int numClients;
        private final int documentsPerClient;
        private final List list;

        public InsertThread(String threadName, DB db, int numClients, int documentsPerClient) {
            this.threadName = threadName;
            this.db = db;
            this.numClients = numClients;
            this.documentsPerClient = documentsPerClient;
            this.list = new ArrayList();
            for (int i = 0; i < documentsPerClient; i++) {
                DBObject o = BasicDBObjectBuilder.start("client", this.threadName).add("docNum", i).add("time", new Date()).get();
                list.add(o);
            }
            System.out.println("client "+threadName+" ready.");
        }

        @Override
        public void run() {
            System.out.println("start client " + threadName);
            long t = System.currentTimeMillis();
            DBCollection samples = db.getCollection("samples");
            samples.insert(list, WriteConcern.NONE);

            float time = (System.currentTimeMillis() - t) / 1000f;
            System.out.println(threadName + " finished after " + time + "secs. given this is the last thread to finish, that means " + ((documentsPerClient * numClients) / time) + " inserts per second.");
        }
    }
}
