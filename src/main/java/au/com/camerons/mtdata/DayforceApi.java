package au.com.camerons.mtdata;

import java.io.PrintStream;
import java.lang.reflect.Type;
import java.util.Base64;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.apache.http.client.config.CookieSpecs;
import org.joda.time.DateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import kong.unirest.HttpRequest;
import kong.unirest.HttpRequestWithBody;

public class DayforceApi {
    public static final String PAGE_SIZE = "pageSize";
    public static final String IS_VALIDATE_ONLY = "IsValidateOnly";
    public static final String EXPAND = "Expand";
    public static final String EMPLOYEES = "Employees";
    public static final String DOCUMENTS = "Documents";
    public static final String CLIENTMETADATA = "ClientMetadata";
    public static final String EMPLOYEE_AVAILABILITY = "Availability";
    public static final String EMPLOYEE_SCHEDULES = "Schedules";
    public static final String EMPLOYEE_TIME_OFF = "TimeAwayFromWork";
    public static final String REPORTMETADATA = "ReportMetadata";
    public static final String REPORT = "Reports";
    public static final String EMPLOYEE_PUNCH = "EmployeePunches";

    private String username;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    private String password;

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    private PrintStream logger;

    public PrintStream getLogger() {
        return logger;
    }

    public void setLogger(PrintStream logger) {
        this.logger = logger;
    }

    private void Log(Object log){
        if (logger!=null)
            logger.println(log);
    }

    private Gson getGsonInstance() {
        return new GsonBuilder().registerTypeAdapter(DateTime.class, new DateTimeTypeAdapter())
                .registerTypeAdapter(byte[].class, new ByteArrayToBase64TypeAdapter())
                .create();
    }


    public <T> T Get(Type returnType, String url, Map<String, Object> queryString) throws UnirestException
    {
        Unirest.config().cookieSpec(CookieSpecs.STANDARD);

        HttpRequest request = Unirest.get(url).basicAuth(getUsername(), getPassword());
        if (queryString != null)
            request = request.queryString(queryString);

        HttpResponse<String> response = request.asString();
        if (response.getStatus()!=HttpStatus.SC_OK){
            Log("Request was not successful, HTTP status code is " + response.getStatusText());
        }
        Gson gson = getGsonInstance();

        return gson.fromJson(response.getBody(), returnType);
    }

    public <T> T Patch(Type returnType, String url, Map<String, Object> queryString, String body) throws UnirestException
    {
        HttpRequestWithBody request = Unirest.patch(url)
                .basicAuth(getUsername(), getPassword())
                .header("content-type", "application/json")
                .queryString(queryString);

        request.body(body);

        HttpResponse<String> response = request.asString();

        if (response.getStatus() == HttpStatus.SC_TEMPORARY_REDIRECT)
        {
            return Patch(returnType, response.getHeaders().getFirst("Location"), null, body);
        }

        if (response.getStatus()!=HttpStatus.SC_OK){
            Log("Request was not successful, HTTP status code is " + response.getStatusText());
        }
        else {
            if (request.getUrl().contains(IS_VALIDATE_ONLY + "=true"))
                Log("Employee was validated successfully");
            else
                Log("Employee was updated successfully");
        }

        if (response.getBody()==null || response.getBody().trim().isEmpty())
            return null;

        Gson gson = getGsonInstance();

        return gson.fromJson(response.getBody(), returnType);
    }

    public <T> T Post(Type returnType, String url, Map<String, Object> queryString, String body) throws UnirestException
    {
        HttpRequestWithBody request = Unirest.post(url)
                .basicAuth(getUsername(), getPassword())
                .header("content-type", "application/json")
                .queryString(queryString);

        request.body(body);

        HttpResponse<String> response = request.asString();

        if (response.getStatus() == HttpStatus.SC_TEMPORARY_REDIRECT)
        {
            return Post(returnType, response.getHeaders().getFirst("Location"), null, body);
        }

        if (response.getStatus()!=HttpStatus.SC_OK){
            Log("Request was not successful, HTTP status code is " + response.getStatusText());
        }
        else {
            if (request.getUrl().contains(IS_VALIDATE_ONLY + "=true"))
                Log("Employee was validated successfully");
            else
                Log("Employee was inserted successfully");
        }

        if (response.getBody()==null || response.getBody().trim().isEmpty())
            return null;

        Gson gson = getGsonInstance();

        return gson.fromJson(response.getBody(), returnType);

    }
    public static class ByteArrayToBase64TypeAdapter implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {
        public byte[] deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return Base64.getDecoder().decode(json.getAsString());
        }

        public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonPrimitive(Base64.getEncoder().encodeToString(src));
        }
    }
    public static class DateTimeTypeAdapter implements JsonSerializer<DateTime>, JsonDeserializer<DateTime> {
        public DateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return new DateTime(json.getAsString());
        }

        public JsonElement serialize(DateTime src, Type typeOfSrc,
                                     JsonSerializationContext context) {
            return new JsonPrimitive(src.toString());
        }
    }
}