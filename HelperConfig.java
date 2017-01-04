
public class HelperConfig {
 String ip;
 int port;
 boolean isBusy;

public HelperConfig(String ip,int port,boolean isBusy){
	super();
	this.ip=ip;
	this.port=port;
	this.isBusy=isBusy;
	
}

@Override
public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((ip == null) ? 0 : ip.hashCode());
	result = prime * result + (isBusy ? 1231 : 1237);
	result = prime * result + port;
	return result;
}

@Override
public boolean equals(Object obj) {
	if (this == obj)
		return true;
	if (obj == null)
		return false;
	if (getClass() != obj.getClass())
		return false;
	HelperConfig other = (HelperConfig) obj;
	if (ip == null) {
		if (other.ip != null)
			return false;
	} else if (!ip.equals(other.ip))
		return false;
	if (isBusy != other.isBusy)
		return false;
	if (port != other.port)
		return false;
	return true;
}


}
