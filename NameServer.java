import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
public class NameServer {
	static String myIp,masterIP;
	static int myPort,masterPort;
	static String gotreq[];
	static boolean Mflag=false;
	static Queue<HelperConfig> inactive_helpers=new LinkedList<HelperConfig>();
	static Queue<HelperConfig> active_helpers=new LinkedList<HelperConfig>();
	//static ArrayList<HelperConfig> inactive_helpers = new ArrayList<HelperConfig>();
	//static ArrayList<HelperConfig> active_helpers = new ArrayList<HelperConfig>();
	static Queue<String> Buff_Q=new LinkedList<String>();
	
	static Runnable portL = new Runnable() {

		public void run() {
			try {
				InetAddress IP=InetAddress.getLocalHost();
				myIp=IP.getHostAddress();
				System.out.println("IP of my system is := "+myIp);
				ServerSocket s = new ServerSocket(0);
				myPort=s.getLocalPort();
				System.out.println("listening on port: " + myPort);
				
				PrintWriter pw = new PrintWriter("ns.txt","UTF-8");
				
				pw.println(myIp + "//" + myPort);
				
				pw.close();
				
				Thread ServiceReqThread = new Thread(ServiceReq);
				ServiceReqThread.start();
				//ServerSocket serverL = new ServerSocket(myPort);
				System.out.println("Name Server Waiting for request");
				while (true) {
					String str;
					
					/*Thread ServiceReqThread = new Thread(ServiceReq);
					ServiceReqThread.start();*/
					Socket socketL = s.accept();
					if (!socketL.isOutputShutdown()) {
						try {
								DataInputStream din = new DataInputStream(socketL.getInputStream());
								str = din.readUTF();
								//System.out.println("Added " + str + "to queue");
								synchronized(this) {
									Buff_Q.add(str);
								}
								//ServiceReqThread.start();
								din.close();
								socketL.close();
							}

						catch (EOFException ex1) {
						}
					}
				}
			} catch (Exception ex1) {
				ex1.printStackTrace();
			}
		}
	};
	static Runnable ServiceReq = new Runnable() {
	public void run() {
		while (true){
			try {
				service();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			}
		}
};
public static void master_send(String msg){
	Socket socket = null;
	if(Mflag){
	try {
		System.out.println("port is "+masterPort);
		socket = new Socket(masterIP, masterPort);

		DataOutputStream d = new DataOutputStream(socket.getOutputStream());
		
		d.writeUTF(msg);
		
		d.close();
		socket.close();
		
	}catch(Exception e){
		e.printStackTrace();
	}
	}
	
}
public static void client_send(String msg,String ip,int port){
	Socket socket = null;
	
	try {
		
		socket = new Socket(ip, port);

		DataOutputStream d = new DataOutputStream(socket.getOutputStream());
		
		d.writeUTF(msg);
		
		d.close();
		socket.close();
		
	}catch(Exception e){
		e.printStackTrace();
	}
	
	
}
public static synchronized void service() throws Exception{
	if(!Buff_Q.isEmpty())
	{
		gotreq=Buff_Q.poll().split("//");
	//System.out.println("message type"+gotreq[0]+"");
		HelperConfig h =null,hc=null;
		String cip;
		int cport;
		String msg="";
		switch(gotreq[0]){
		case "Helper":
			System.out.println("Received Request from Helpers");
			hc = new HelperConfig (gotreq[1],Integer.parseInt(gotreq[2]),false);
			System.out.println("before object "+Integer.parseInt(gotreq[2]));
			inactive_helpers.add(hc);
			System.out.println(inactive_helpers.size()+" "+inactive_helpers.peek().port+" "+hc.port+"helooo");
			for(HelperConfig item : inactive_helpers){
			    System.out.println(item.port);
			}

			updater();
			break;
		case "M_register":
			System.out.println("Received Request from Master for registration");
			masterIP =gotreq[1];
			masterPort = Integer.parseInt(gotreq[2]);
			System.out.println("Master ip and port is : "+masterIP+" "+masterPort);
			Mflag=true;
			while(!active_helpers.isEmpty()){
				inactive_helpers.add(active_helpers.poll());
			}
			updater();
			break;
		case "M_help":
			System.out.println("received help req");
			System.out.println(inactive_helpers.size()+" "+inactive_helpers.peek().port);
			System.out.println("Received Request from Master for helpers");
			if(!inactive_helpers.isEmpty()){
				msg+="NS_ack_help"+"//"+inactive_helpers.size();
				while(!inactive_helpers.isEmpty()){
					System.out.println(inactive_helpers.size()+" "+inactive_helpers.peek().port);
					h=inactive_helpers.poll();
					//System.out.println("in "+h.port);
					active_helpers.add(h);
					msg+="//"+h.ip+"//"+h.port;
					
				}
				System.out.println("final reply is:"+msg);
				master_send(msg);
				updater();
				break;
			}
		case "getMaster":
			cip=gotreq[1];
			cport=Integer.parseInt(gotreq[2]);
			msg="NS_reply"+"//"+masterIP+"//"+masterPort;
			client_send(msg,cip,cport);
			break;
		default :break;
			
			
			
			
			
		}
	
			
		}
	
}
public static void updater() throws Exception, UnsupportedEncodingException{
	PrintWriter po = new PrintWriter("nsLog.txt","UTF-8");
	
	
	po.println("master" + "//"+masterIP+"//" + masterPort);
	//po.println("active");
	for(HelperConfig item : active_helpers){
	    po.println("active//"+item.ip+"//"+item.port);
	}
	for(HelperConfig item : inactive_helpers){
	    po.println("inactive//"+item.ip+"//"+item.port);
	}
	
	po.close();
	
}

public static void main(String args[]) throws IOException{
	
	Thread PortLThread = new Thread(portL);
	PortLThread.start();
	
	
}

}

