package au.com.camerons.mtdata.model;

import com.google.gson.annotations.SerializedName;

public class Paging {
	
    /// <summary>
    /// This property contains the link to the next page of data in a paginated response
    /// Empty string indicates that there is no more page left
    /// </summary>
	@SerializedName("Next")
    private String next;
	
	public String getNext(){
		return this.next;
	}
	
	public void setNext(String next){
		this.next = next;
	}
}
