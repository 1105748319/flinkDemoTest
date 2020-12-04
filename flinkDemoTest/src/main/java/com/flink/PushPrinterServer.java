package com.flink;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class PushPrinterServer  {
	public static void main(String[] args) throws Exception {
		int port = args.length > 0 ? Integer.parseInt(args[0]) : 19999;
		ServerSocket server = new ServerSocket(port);
		System.out.println("server waits client");
		Socket client = server.accept();
		System.out.println("client comes. please write to client");
		System.out.printf("sfsdfasfdddddddddddd");

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));

		BufferedReader consoleRdr = new BufferedReader(new InputStreamReader(System.in));
		String line;
		do {
			line = consoleRdr.readLine();
			if ("quit".equals(line)) {
				break;
			}

			bw.write(line);
			bw.write("\r\n");
			bw.flush();
		} while (true);

		bw.close();
		server.close();

		System.out.println("bye");
	}


}
