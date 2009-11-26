import java.io.*;
import java.net.*;
import clojure.lang.*;
import fleetdb.Serializer;

public class BinaryClient {
  public static class ClientThread extends Thread {
    private final Socket socket;
    private final DataInputStream dis;
    private final DataOutputStream dos;
    private final PersistentVector query;
    private final Object eof;
    private final int hits;
    
    
    public ClientThread(String host, int port, int hits) throws Exception {
      this.socket = new Socket(host, port);
      this.dis  = new DataInputStream( new BufferedInputStream( socket.getInputStream()));
      this.dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
      this.hits = hits;
      this.query = PersistentVector.create(Keyword.intern(null, "ping"));
      this.eof = new Object();
    }
 
    public void run() {
      try {
        for (int h = 0; h < this.hits; h++) {
          Serializer.serialize(this.dos, this.query);
          this.dos.flush();
          Serializer.deserialize(this.dis, this.eof);
        }   
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis();
    Thread client = new ClientThread("localhost", 4444, 100000);
    client.start();
    client.join();
    long end = System.currentTimeMillis();
    System.out.println(end - start);
    
    start = System.currentTimeMillis();
    Thread[] threads = new Thread[10];
    for (int t = 0; t < 10; t++) {
      threads[t] = new ClientThread("localhost", 4444, 10000);
      threads[t].start();
    }
    for (int t = 0; t < 10; t++) {
      threads[t].join();
    }
    end = System.currentTimeMillis();
    System.out.println(end - start);
  }
}