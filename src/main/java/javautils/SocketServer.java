package javautils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer {

	public static void main(String[] args) {
		try {

			// input file name;
			// String inputfilename="data/test.dat";
			String inputfilename = args[0];

			ServerSocket serverSocket = new ServerSocket(
					Integer.parseInt(args[1]));

			while (true) {
				//
				Socket socket = serverSocket.accept();

				PrintWriter printWriter = new PrintWriter(
						socket.getOutputStream());
				BufferedReader br = null;
				// int line = 0;

				try {

					String sCurrentLine;

					br = new BufferedReader(new FileReader(inputfilename));

					while ((sCurrentLine = br.readLine()) != null) {

						printWriter.println(sCurrentLine);
						printWriter.flush();

						// sleep time
						Thread.sleep(Integer.parseInt(args[2]));
					}

				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						if (br != null)
							br.close();
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}

				/** close Socket */
				printWriter.close();

				socket.close();
			}
		} catch (Exception e) {
			System.out.println("Exception:" + e);
		} finally {
			// serverSocket.close();
		}
	}
}