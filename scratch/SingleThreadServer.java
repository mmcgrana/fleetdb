package fleetdb;

import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class SingleThreadServer {
  public static void main(String[] args) throws IOException {
    ServerSocket serverSocket = new ServerSocket(4444, 10000);
    while (true) {
      Socket socket = (serverSocket.accept());
      try {
        PrintWriter out = new PrintWriter(socket.getOutputStream());
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        in.readLine();
        out.println("HTTP/1.0 200 OK");
        out.println();
        out.println("Content-Length: 1354");
        out.println("<html>hi</html>");
        out.close();
        in.close();
        socket.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
