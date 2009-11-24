import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class MultiThreadPersistentClient {
  public static class ClientHandler extends Thread {
    private final Socket socket;
    public ClientHandler(Socket socket) {this.socket = socket; }
    
    public void run() {
      int hits = 2000;
      try {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        for (int i = 0; i < hits; i++) {
          if (i % 1000 == 0) { System.out.print("."); System.out.flush(); }
          out.println("ping");
          String msg = in.readLine();
          if (!msg.equals("pong")) {
            System.err.print("Unexpected response: ");
            System.err.println(msg);
            System.err.flush();
            System.exit(1);
          }
        }
        in.close();
        out.close();
        socket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    int trials = 2;
    int conns = 50;
    for (int t = 0; t < trials; t++) {
      long start = System.currentTimeMillis();
      Thread[] threads = new Thread[conns];
      for (int c = 0; c < conns; c++) {
        threads[c] = new ClientHandler(new Socket("localhost", 4444));
        threads[c].start();
      }
      for (int c = 0; c < conns; c++) {
        threads[c].join();
      }
      long end = System.currentTimeMillis();
      System.out.println();
      System.out.println(end-start); 
    }
  }
}