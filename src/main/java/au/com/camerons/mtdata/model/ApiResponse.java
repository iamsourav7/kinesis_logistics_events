
package au.com.camerons.mtdata.model;

import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

/**
 * API response returned by API call.
 *
 * @param T The type of data that is de serialized from response body
 */
public class ApiResponse<T> {
	@SerializedName("StatusCode")
    private int statusCode;
	@SerializedName("Headers")
    private Map<String, List<String>> headers;
	@SerializedName("Data")
    private T data;
	@SerializedName("requestId")
	private String requestId;
	@SerializedName(value = "processResults", alternate = {"ProcessResults"})
	private List<ProcessResult> processResults;
	@SerializedName("Paging")
	private Paging paging;
	
	public int getStatusCode() {
		return statusCode;
	}
	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}
	public Map<String, List<String>> getHeaders() {
		return headers;
	}
	public void setHeaders(Map<String, List<String>> headers) {
		this.headers = headers;
	}
	public T getData() {
		return data;
	}
	public void setData(T data) {
		this.data = data;
	}
	public String getRequestId() {
		return requestId;
	}
	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}
	public List<ProcessResult> getProcessResults() {
		return processResults;
	}
	public void setProcessResults(List<ProcessResult> processResults) {
		this.processResults = processResults;
	}
	public Paging getPaging() {
		return this.paging;
	}
	public void setPaging(Paging paging) {
		this.paging = paging;
	}
}
