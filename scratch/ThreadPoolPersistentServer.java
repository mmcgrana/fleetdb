import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class ThreadPoolPersistentServer {
  
  private static class ServerHandler implements Runnable {
    private final Socket socket;
    public ServerHandler(Socket socket) { this.socket = socket; }
    
    public void run() {
      try {
        PrintWriter out = new PrintWriter(socket.getOutputStream());
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        while (true) {
          String msg = in.readLine();
          if (msg.equals("ping")) {
            out.println("pong");
            out.flush();
          } else {
            System.err.print("Unexpected request: ");
            System.err.println(msg);
            System.err.flush();
            System.exit(1);
          } 
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  public static void main(String[] args) throws IOException {
    ServerSocket serverSocket = new ServerSocket(4444, 10000);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    
    while (true) {
      executor.execute(new ServerHandler(serverSocket.accept()));
    }
  }
}
