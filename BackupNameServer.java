import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

public class BackupNameServer {
	static String myIp,masterIP,nsIp;
	static int myPort,masterPort,nsPort;
	static String gotreq[],ipport[];
	static ServerSocket s;
	static boolean Mflag=false,stopper=true;
	static Queue<HelperConfig> inactive_helpers=new LinkedList<HelperConfig>();
	static Queue<HelperConfig> active_helpers=new LinkedList<HelperConfig>();
	//static ArrayList<HelperConfig> inactive_helpers = new ArrayList<HelperConfig>();
	//static ArrayList<HelperConfig> active_helpers = new ArrayList<HelperConfig>();
	static Queue<String> Buff_Q=new LinkedList<String>();
	
	
	
	static Runnable HeartBeat = new Runnable() {
		public void run() {
			System.out.println("HeartBeat started");
			while (stopper) {
				try {
				Socket socket = null;
				socket = new Socket(nsIp, nsPort);
				if(!socket.isOutputShutdown())
				{
					DataOutputStream d = new DataOutputStream(socket.getOutputStream());
					
					d.writeUTF("HeartBeat//"+myIp+"//"+myPort);
					
					d.close();
					socket.close();
					Thread.sleep(5000);
						
						
				}else{
					
				}
				}	catch(IOException | InterruptedException e){
					System.out.println("NameServer failed");
					try {
						reader();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					Thread PortLThread = new Thread(portL);
					PortLThread.start();
					stopper=false;
				}
				
			}
		}
	};
	public static void init(){
		InetAddress IP;
		try {
			IP = InetAddress.getLocalHost();
		
		myIp = IP.getHostAddress();
		System.out.println("IP of my system is := " + myIp);
		s = new ServerSocket(0);
		myPort = s.getLocalPort();
		System.out.println("listening on port: " + myPort);
		BufferedReader br = new BufferedReader(new FileReader("ns.txt"));
		String line = br.readLine();
		ipport = line.split("//");
		nsIp = ipport[0];
		nsPort = Integer.parseInt(ipport[1]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//---------------------------------------------------------------------------------------------------
	
	static Runnable portL = new Runnable() {

		public void run() {
			try {
				/*InetAddress IP=InetAddress.getLocalHost();
				myIp=IP.getHostAddress();
				System.out.println("IP of my system is := "+myIp);
				ServerSocket s = new ServerSocket(0);
				myPort=s.getLocalPort();
				System.out.println("listening on port: " + myPort);*/
				
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
public static void reader() throws Exception{
	String line[];
	Scanner in=new Scanner (new File("nsLog.txt"));
	while(in.hasNext()){
		line=in.nextLine().split("//");
		switch(line[0]){
		case "master":
			masterIP=line[1];
			masterPort=Integer.parseInt(line[2]);
			System.out.println("Master ipp and port is "+masterIP+" "+masterPort);
			break;
		case "active":
			HelperConfig h = new HelperConfig(line[1], Integer.parseInt(line[2]), true);
			active_helpers.add(h);
			break;
		case "inactive":
			HelperConfig h1 = new HelperConfig(line[1], Integer.parseInt(line[2]), true);
			active_helpers.add(h1);
			break;
			
		
		}
		
	}
}

public static void main(String args[]) throws Exception{
	
	init();
	Thread HeartBeatThread = new Thread(HeartBeat);
	HeartBeatThread.start();
	
	
	
}

}


	
	


