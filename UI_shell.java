import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class UI_shell {
	static int type;
	static String path;
	static String keywords[];
	static String myIP,nsIP,msIP;
	static int myPort,nsPort,msPort;
	static String ipport[];
	static Scanner in = new Scanner(System.in);
	static ServerSocket s;
	public static void main(String args[]){
		try {
			init();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void init(){
		InetAddress IP;
		try {
			IP = InetAddress.getLocalHost();
		
		myIP=IP.getHostAddress();
		System.out.println("IP of my system is := "+myIP);
		 s = new ServerSocket(0) ;
		myPort=s.getLocalPort();
		System.out.println("listening on port: " + myPort);
		BufferedReader b = new BufferedReader (new FileReader ("ns.txt"));
		String line=b.readLine();
		ipport= line.split("//");
		nsIP= ipport[0];
		nsPort=Integer.parseInt(ipport[1]);
		System.out.println("ns "+nsIP+""+nsPort);
		Thread clientListenerThread = new Thread(clientListener);
		clientListenerThread.start();
		makeRequest(0);
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			

		} 
	}
	
	public static void send(String msg,String ip,int port){
		Socket socket = null;
		try {
			socket = new Socket(ip, port);
	
			DataOutputStream d = new DataOutputStream(socket.getOutputStream());
			
			d.writeUTF(msg);
			
			d.close();
			socket.close();
			
		} catch (Exception e) {
			BufferedReader b = new BufferedReader (new FileReader ("ns.txt"));
		String line=b.readLine();
		ipport= line.split("//");
		nsIP= ipport[0];
		nsPort=Integer.parseInt(ipport[1]);
Systme.out.println("Master or NameServer Failed");
Display();	
			}
	}
	
	public static void Display() throws IOException{ 
		
		String temp;
		BufferedReader br = new BufferedReader (new InputStreamReader(System.in));
		System.out.println("Enter type of request 1. Indexing 2. Search 3. Retrieve");
		type=in.nextInt();
		switch(type){

		case 1:
			System.out.println("Please Enter the path");
			path = br.readLine();
			System.out.println("path is : "+path);
			makeRequest(1);
			Display();
			
			
			break;
		case 2:
			System.out.println("Please Enter keywords");
			temp = br.readLine();
			keywords = temp.split(" ");
			System.out.println("keywords are :");
			for(int i=0;i<keywords.length;i++)
				System.out.println(keywords[i]);
			makeRequest(2);
			break;
		case 3:
			break;
			
		}
		//System.out.println("path is : "+path);
		
	}
	
	public static void makeRequest(int type){
		System.out.println("in make req");
		String msg;
		String kw = "";
		switch(type){
		case 0:
			msg="getMaster"+"//"+myIP+"//"+myPort;
			Socket socket = null;
			send(msg,nsIP,nsPort);
			/*try {
				socket = new Socket(nsIP, nsPort);
		
				DataOutputStream d = new DataOutputStream(socket.getOutputStream());
				
				d.writeUTF(msg);
				
				d.close();
				socket.close();
				
			} catch (Exception e) {
				e.printStackTrace();	
				}*/
			break;
		case 1:
			msg="index"+"#"+path+"#"+myIP+"#"+myPort;
			send(msg,msIP,msPort);
			break;
		case 2:
			for(int i=0;i<keywords.length;i++)
			{
				 kw = kw+"//"+keywords[i];
			}
			msg="search"+"//"+keywords.length+"//"+kw+"//"+myIP+"//"+myPort;
			send(msg,msIP,msPort);
			break;
		}
	   
		
				
			}
		
	
	
	static Runnable clientListener = new Runnable() {
	    
	    String temp[],reply[];
		Socket socketL;
		@Override
		public void run() {
			System.out.println("Client Listening");
			while (true) {
				String str;
				
				
			
				try {
					socketL = s.accept();
					DataInputStream din = new DataInputStream(socketL.getInputStream());
					
					
				if (!socketL.isOutputShutdown()) {
					
					str = din.readUTF();
					if (str.contains("#")){
						reply=str.split("#");
						System.out.println("Received Reply :");
						System.out.println(reply[1]);
						Display();
						
					}
					temp=str.split("//");
					//System.out.println("Client got : "+str);
					switch(temp[0]){
					case "NS_reply":msIP=temp[1];
						msPort=Integer.parseInt(temp[2]);
						System.out.println("master ip and port : "+msIP+" "+msPort);
						Display();
						break;
						
						
					}
				
				}
				
				din.close();
				}catch(Exception e){
					e.printStackTrace();
				}
			}
			
			
		}
	};
	


}
