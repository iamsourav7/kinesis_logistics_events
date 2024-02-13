package au.com.camerons.mtdata.kinesis.logistics;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;

public class Utils {

    public static java.util.Date tryParseDate(String value, java.util.Date defaultVal) {
        try {

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            sdf.setLenient(true);
            return sdf.parse(value);
        } catch (DateTimeParseException | ParseException e) {
            return defaultVal;
        }
    }

    public static StringBuilder safeStringToStringBuilder(String value) {
        try {
            StringBuilder sb = new StringBuilder(value);
            return sb;
        } catch (NullPointerException e) {
            return null;
        }
    }

    public static int tryParseInt(String value, int defaultVal) {
        try {

            // Added Double handing to deal with MTData changing quantity string from integer to decimal
            double d = Double.parseDouble(value);
            int i = (int) d;
            // return Integer.parseInt(value);
            return i;
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    public static float tryParseFloat(String value, float defaultVal) {
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            return defaultVal;
        } catch (NullPointerException e) {
            return defaultVal;
        }
    }
}
