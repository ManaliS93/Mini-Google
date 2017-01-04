import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Scanner;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Helper {
	static String myIp,nsIp;
	static int myPort,nsPort;
	static String ipport[];
	static ServerSocket s;
	static Queue<String> index_q=new LinkedList<String>();
	static Queue<String> search_q=new LinkedList<String>();
	static ArrayList <String> arr_index=new ArrayList<String>();
	
	public static void init() throws IOException{
		
		InetAddress IP=InetAddress.getLocalHost();
		myIp=IP.getHostAddress();
		System.out.println("IP of my system is := "+myIp);
		s = new ServerSocket(0);
		myPort=s.getLocalPort();
		System.out.println("listening on port: " + myPort);
		BufferedReader br = new BufferedReader (new FileReader ("ns.txt"));
		String line=br.readLine();
		ipport= line.split("//");
		nsIp= ipport[0];
		nsPort=Integer.parseInt(ipport[1]);
		System.out.println(nsIp+""+nsPort);
		Thread ServerListenerThread = new Thread(ServerListener);
		ServerListenerThread.start();
		Thread ServiceIndexThread = new Thread(ServiceIndex);
		ServiceIndexThread.start();
		Thread ServiceSearchThread = new Thread(ServiceSearch);
		ServiceSearchThread.start();
		Socket socket = null;
		try {
			socket = new Socket(nsIp, nsPort);
	
			DataOutputStream d = new DataOutputStream(socket.getOutputStream());
			
			d.writeUTF("Helper//"+myIp+"//"+myPort);
			
			d.close();
			socket.close();
			
		} catch (ConnectException e) {
			e.printStackTrace();	
			}
	
	}
	
	static Runnable ServerListener = new Runnable() {

		@Override
		public void run() {
			while (true) {
				String str;
				String temp[];
				System.out.println("Helper Waiting for request");
				
				Socket socketL;
				try {
					socketL = s.accept();
				
				if (!socketL.isOutputShutdown()) {
					try {
						DataInputStream din = new DataInputStream(socketL.getInputStream());
						str = din.readUTF();
						//System.out.println("Helper got : "+str);
						temp=str.split("#");
						switch(temp[0]){
						case "path":
						case "-+-":
							synchronized(this) {
								index_q.add(str);
							}
							break;
							
						case "search":
							synchronized(this) {
								search_q.add(str);
							}
							break;
						
							
						
						}
						
						
						//ServiceReqThread.start();
						din.close();
						socketL.close();
					}

				catch (EOFException ex1) {
				}
				}
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}
	};
					
	static Runnable ServiceIndex = new Runnable() {
		public void run() {
			System.out.println("Service Index started");
			while (true){
				try {
					index_task();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				}
			}
	};
	static Runnable ServiceSearch = new Runnable() {
		public void run() {
			System.out.println("Service search started");
			while (true){
				try {
					search_task();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				}
			}
	};
public static synchronized void index_task() throws InterruptedException{
		String item,path;int freq;
		String temp[] = null;
		IndexStore is=null;
		//HashMap<String,IndexStore> Map = new HashMap<String,IndexStore>();
		Map<String, List<IndexStore>> map = new HashMap<String, List<IndexStore>>();
		List<IndexStore> arl=new ArrayList<IndexStore>();
		//System.out.println("inside index task");
		if(!index_q.isEmpty()){
			System.out.println("inside index task");
			temp=index_q.poll().split("#");
			System.out.println(""+temp[0]);
			
			System.out.println(temp[0]+" - "+temp[1]+" - "+temp[2]+" - ");
			String mip=temp[3];
			int mport = Integer.parseInt(temp[4]);
			path=temp[1];
			System.out.println("path found as : "+path);
			temp[2]=temp[2].replaceAll("([^a-zA-Z']+)'*\\1*", " ") ;
			temp[2]=temp[2].replaceAll("\\s", " ") ;
			temp[2]=temp[2].replaceAll("  ", "") ;
				List<String> list = new ArrayList<String>(Arrays.asList(temp[2].toLowerCase().split(" ")));
				if (list.contains(" "))
				{int jk=list.indexOf(" ");
				
				list.remove(jk);}
			for(int i=0;i<list.size();i++)      
			 {        
				 item=list.get(i);
				 if (item.equals(""))
				 {
					 continue;
				 }
					
			   freq = Collections.frequency(list, list.get(i));
			   //System.out.println(item+" "+freq);
			   
			  
			   is=new IndexStore(path,freq);
			   if (map.containsKey(item)){
				   arl=map.get(item);
				   if (arl.contains(is))
				   {	
					  // System.out.println("arl contains : "+is.path+" for "+item);
					  // System.out.println(arl.get(0).path+" for key "+item);
					   //arl.get(arl.indexOf(is)).value=arl.get(arl.indexOf(is)).value+is.value;
					   }else{
						  // System.out.println("arl DOES NOT contain : "+is.path+" for "+item);
						   arl.add(is);
					   }
				   map.put(item, arl);
			   }else{
				   arl=new ArrayList<IndexStore>();
				   //System.out.println("first time added "+is.path+ " for "+item);
				   arl.add(is);
				   map.put(item, arl);
			   }
			   
			 }
		
			
			 for (Entry<?, ?> e : map.entrySet())
			    {
			        //System.out.println("Key: " + e.getKey() + " Value: " );
			        List<IndexStore> var = (List<IndexStore>) e.getValue();
			        for(int i=0; i<var.size(); i++)
			        {
			        	IndexStore tempIS = var.get(i);
			        	//System.out.println("Path: " + tempIS.path + " - Value: " + tempIS.value);
			        	String msg="resultIndex"+"@"+myIp+"@"+myPort+"@"+e.getKey()+"@"+tempIS.path+"@"+tempIS.value;
			        	master_send(msg, mip, mport);
			        	 
			        }

			    }
			DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
      					 Date dateobj = new Date();
      					 System.out.println("----Time-----"+df.format(dateobj));
				
			
			
		}
		}

public static synchronized void search_task() throws Exception{
	String temp[] = null;
	String temp1[] = null;
	String arr_search[]=null;
	String line,m="";
	String msg="";
	int num=0;
	if(!search_q.isEmpty()){
		System.out.println("inside method search **********");
		temp=search_q.poll().split("#");
		temp1=temp[1].split("//");
		for(int i=1;i<temp1.length;i++){
			if(temp1[i].equals(""))
				continue;
		System.out.println("----"+temp1[i]);
		String letter = temp1[i].substring(0, 1);
		System.out.println("letter is "+letter);
		Scanner sc = new Scanner (new File ("/afs/cs.pitt.edu/usr0/mcs116/public/MIndex"+"/"+letter+".txt"));
		while(sc.hasNext())
		{
			line=sc.nextLine();
			if(line.contains("#"))
			{
			arr_search= line.split("#");
			
				System.out.println("arr search : "+line);
			if (temp1[i].equals(arr_search[1].toLowerCase()))
			{	num++;
				msg+="#"+arr_search[1]+"!!"+arr_search[2]+"!!"+arr_search[3];
				//msg+="#"+arr_search[2];
				//break;
			}
			}else continue;
		}
			
			
		
		
		
		}
		//System.out.println(msg);
		 m="resultSearch"+"@"+temp[4]+"@"+msg;
		// System.out.println("Sending result to master "+ m);
   	master_send(m, temp[2], Integer.parseInt(temp[3]));
		
	}
	
}
public static synchronized void master_send(String msg,String mip,int mport) throws InterruptedException{
	Socket socket = null;
	Thread.sleep(700);
	try {
		//System.out.println("port is "+mport);
		socket = new Socket(mip, mport);

		DataOutputStream d = new DataOutputStream(socket.getOutputStream());
		
		d.writeUTF(msg);
		
		d.close();
		socket.close();
		
	}catch(Exception e){
		e.printStackTrace();
	}
	
	
	
}
	public static void main(String arg[]){
		try {
			init();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
	

}
