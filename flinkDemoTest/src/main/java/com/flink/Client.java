package com.flink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client {
	public static void main(String[] args) throws UnknownHostException, IOException {
		final int PORT = 19999;
		Socket client = new Socket("localhost", PORT);

		BufferedReader rdr = new BufferedReader(new InputStreamReader(client.getInputStream()));
		do {
			String line = rdr.readLine();
			System.out.print("server said:");
			System.out.println(line);
		} while (true);
	}

}
