import java.util.HashMap;

public class IndexStore implements Comparable{
	// HashMap<String,Integer> path_count = new HashMap<String,Integer>();
	 String path;
	 int value;
	 
	 public IndexStore(String path,int value){
			super();
			this.path=path;
			this.value=value;
		}
	 public void to_String(){
		 System.out.println("path "+path+" value "+value);
		 
	 }
	/* public int compare(IndexStore i1) {
	        return i1.value;
	    }*/
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		result = prime * result + value;
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
		IndexStore other = (IndexStore) obj;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		if (value != other.value)
			return false;
		return true;
	}
	
	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return this.value;
	}

}
