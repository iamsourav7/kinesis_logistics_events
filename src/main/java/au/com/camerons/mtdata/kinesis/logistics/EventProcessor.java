package au.com.camerons.mtdata.kinesis.logistics;

import com.google.gson.*;
import com.microsoft.sqlserver.jdbc.SQLServerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.lang3.builder.CompareToBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

import static au.com.camerons.mtdata.kinesis.logistics.Queries.jobActionCompleteSql;

@SpringBootApplication
@SuppressWarnings("unchecked")
public class EventProcessor implements CommandLineRunner {

	@Autowired
	private Environment env;

	@Autowired
	private DataSource dataSource;

	private final ObjectMapper jsonObjectMapper = new ObjectMapper();

	private static final Logger log = LoggerFactory.getLogger(EventProcessor.class);

	private static int pollingInterval;
	private static int pollingRecordCount;

	private static String getEventsSql;

	private static final Pattern patternJobReferenceWaveId = Pattern.compile("[A-Z]\\w+-[0-9]\\w+-C[0-9]\\w+");
	private static final Pattern patternJobReferenceSONumber = Pattern.compile("SO[0-9]+");
	private static final Pattern patternJobReferenceExtractWaveId = Pattern.compile("([^-]+-[^-]+)");
	private static final Pattern patternJobReferenceExtractDeliverId = Pattern.compile("([^-]*$)");

	public static void main(String[] args) {
		// LoadProperties();
		SpringApplication.run(EventProcessor.class, args);
	}

	public void run(String... args) {
		log.info("Running");
		log.info("Connection Polling datasource : " + dataSource);  // check connection pooling

		pollingInterval = Utils.tryParseInt(env.getProperty("polling.milliseconds", "1000"), 1000);
		pollingRecordCount = Utils.tryParseInt(env.getProperty("polling.records", "1000"), 1000);

		log.info("Data Polling Interval (Milliseconds) : " + Integer.toString(pollingInterval));
		log.info("Data Polling Max Records Per Batch : " + Integer.toString(pollingRecordCount));

		getEventsSql =
				"SELECT TOP " + pollingRecordCount +
						"       [partitionKey]" +
						"      ,[sequenceNumber]" +
						"      ,[data]" +
						"      ,[sequence]" +
						"      ,[inserted_timestamp]" +
						"      ,[parsed_report_id]" +
						"      ,[parsed_fleet_id]" +
						"      ,[parsed_vehicle_id]" +
						"      ,[parsed_driver_id]" +
						"      ,[parsed_gps_time]" +
						"      ,[parsed_device_time]" +
						"      ,[parsed_server_time]" +
						"  FROM [MTDATA_KINESIS].[dbo].[events_logistics_raw]\n" +
						"  WHERE parsed_report_id IN (14)" +
						"      AND sequence > ?" +
						"  ORDER BY" +
						"      sequence "
		;

		log.debug("Polling Query Used: ");
		log.debug(getEventsSql);

		ProcessLogisticsEvents();
	}

	private void ProcessLogisticsEvents() {

		log.info("Processing Logistics Events");

		Date defaultDate = new java.sql.Date(0);

		// Set Event Store Low Watermark
		// This is the oldest allowed starting point for the fetch loop.
		String lastWatermarkSql = "SELECT COALESCE(last_id, 0) last_id FROM [MTDATA_KINESIS].dbo.watermark WHERE table_name = 'events_logistics_raw'";

		ResultSet eventsResultSet = null;
		BigInteger watermark = BigInteger.valueOf(0);

		try {

			// Polling Loop
			while (!Thread.currentThread().isInterrupted()) {

				// Get Watermark
				try (
						Connection connection = dataSource.getConnection();
						PreparedStatement prepsSelectWatermark = connection.prepareStatement(lastWatermarkSql);
						ResultSet watermarkResultSet = prepsSelectWatermark.executeQuery();
				) {
					while (watermarkResultSet.next()) {
						watermark = watermarkResultSet.getBigDecimal(1).toBigInteger();
					}
				}
				catch (SQLIntegrityConstraintViolationException sicve) {
					// NOP
				}
				catch (SQLServerException sqle) {
					log.error(sqle.getMessage());
					log.error(sqle.getSQLState());
				}
				catch (Exception e) {
					log.error("SQL Error:", e);
				}

				try ( Connection connection = dataSource.getConnection();
					  PreparedStatement prepsEvents = connection.prepareStatement(getEventsSql);
				) {

					prepsEvents.setBigDecimal(1, new BigDecimal(watermark));

					eventsResultSet = prepsEvents.executeQuery();

					HashMap eventHashMap = null;
					HashMap jobData = null;

					ArrayList legData = null;

					boolean result = false;
					BigInteger eventSequence;

					StringBuilder eventJson = new StringBuilder();

					StringBuilder report_id = new StringBuilder();

					StringBuilder fleet_id = new StringBuilder();
					StringBuilder fleet_name = new StringBuilder();
					StringBuilder vehicle_id = new StringBuilder();
					StringBuilder vehicle_name = new StringBuilder();
					StringBuilder vehicle_external_reference = new StringBuilder();
					StringBuilder driver_id = new StringBuilder();
					StringBuilder driver_name = new StringBuilder();
					StringBuilder driver_external_reference = new StringBuilder();

					StringBuilder dispatch_id = new StringBuilder();
					StringBuilder job_reference = new StringBuilder();
					StringBuilder job_reference_int = new StringBuilder();
					StringBuilder job_title = new StringBuilder();
					StringBuilder docket_number = new StringBuilder();
					StringBuilder job_type = new StringBuilder();
					StringBuilder job_status = new StringBuilder();
					StringBuilder leg_id = new StringBuilder();
					StringBuilder event_id = new StringBuilder();
					StringBuilder report_leg_id = new StringBuilder();

					StringBuilder gps_time = new StringBuilder();
					StringBuilder server_time = new StringBuilder();
					StringBuilder device_time = new StringBuilder();
					StringBuilder latitude = new StringBuilder();
					StringBuilder longitude = new StringBuilder();
					StringBuilder speed_accumulator = new StringBuilder();
					StringBuilder direction = new StringBuilder();
					StringBuilder speed = new StringBuilder();
					StringBuilder maximum_speed = new StringBuilder();
					StringBuilder place_name = new StringBuilder();
					StringBuilder map_reference = new StringBuilder();
					StringBuilder hdop = new StringBuilder();
					StringBuilder num_satellites_in_view = new StringBuilder();
					StringBuilder altitude = new StringBuilder();

					StringBuilder set_point_id = new StringBuilder();
					StringBuilder set_point_name = new StringBuilder();
					StringBuilder waypoint_external_ref = new StringBuilder();

					StringBuilder address_number = new StringBuilder();
					StringBuilder address_street_name = new StringBuilder();
					StringBuilder address_suburb = new StringBuilder();
					StringBuilder address_city = new StringBuilder();
					StringBuilder address_state = new StringBuilder();
					StringBuilder address_postal_code = new StringBuilder();
					StringBuilder address_country = new StringBuilder();
					StringBuilder address_timezone_offset = new StringBuilder();

					StringBuilder odometer = new StringBuilder();
					StringBuilder total_fuel = new StringBuilder();

					StringBuilder device_serial_number = new StringBuilder();
					StringBuilder device_id = new StringBuilder();

					if (eventsResultSet.isBeforeFirst()) {
						log.info("starting the reportId parsing");
						while (eventsResultSet.next()) {

							// Clear String Buffer Working Vars
							eventJson.setLength(0);

							report_id.setLength(0);

							fleet_id.setLength(0);
							fleet_name.setLength(0);
							vehicle_id .setLength(0);
							vehicle_name.setLength(0);
							vehicle_external_reference.setLength(0);
							driver_id.setLength(0);
							driver_name.setLength(0);
							driver_external_reference.setLength(0);

							dispatch_id.setLength(0);
							job_reference.setLength(0);
							job_reference_int.setLength(0);
							job_title.setLength(0);
							docket_number.setLength(0);
							job_type.setLength(0);
							job_status.setLength(0);
							leg_id.setLength(0);
							event_id.setLength(0);
							report_leg_id.setLength(0);

							gps_time.setLength(0);
							server_time.setLength(0);
							device_time.setLength(0);
							latitude.setLength(0);
							longitude.setLength(0);
							speed_accumulator.setLength(0);
							direction.setLength(0);
							speed.setLength(0);
							maximum_speed.setLength(0);
							place_name.setLength(0);
							map_reference.setLength(0);
							hdop.setLength(0);
							num_satellites_in_view.setLength(0);
							altitude.setLength(0);

							set_point_id.setLength(0);
							set_point_name.setLength(0);
							waypoint_external_ref.setLength(0);

							address_number.setLength(0);
							address_street_name.setLength(0);
							address_suburb.setLength(0);
							address_city.setLength(0);
							address_state.setLength(0);
							address_postal_code.setLength(0);
							address_country.setLength(0);
							address_timezone_offset.setLength(0);

							odometer.setLength(0);
							total_fuel.setLength(0);

							device_serial_number.setLength(0);
							device_id .setLength(0);

							log.debug("Processing sequence: " + eventsResultSet.getInt(4));
							eventSequence = eventsResultSet.getBigDecimal(4).toBigInteger();
							eventJson.append((String) eventsResultSet.getString(3));

							Boolean continueProcessing = true;

							try {
								eventHashMap = createHashMapFromJsonString(eventJson.toString());
							} catch (Exception jsonParseException) {
								log.error("JSON Parse Exception", jsonParseException);
							}

							if (eventHashMap != null) {

								report_id.append((String) eventHashMap.get("ReportId"));

								fleet_id.append((String) eventHashMap.get("FleetId"));
								fleet_name.append((String) eventHashMap.get("FleetName"));
								vehicle_id.append((String) eventHashMap.get("VehicleId"));
								vehicle_name.append((String) eventHashMap.get("VehicleName"));
								vehicle_external_reference.append((String) eventHashMap.get("VehicleExternalRef"));
								driver_id.append((String) eventHashMap.get("DriverId"));
								driver_name.append((String) eventHashMap.get("DriverName"));
								driver_external_reference.append((String) eventHashMap.get("DriverExternalRef"));

								odometer.append((String) eventHashMap.get("Odometer"));
								total_fuel.append((String) eventHashMap.get("TotalFuel"));

								device_serial_number.append((String) eventHashMap.get("DeviceSerialNumber"));
								device_id.append((String) eventHashMap.get("DeviceId"));


								// for gps data

								if (eventHashMap.containsKey("GpsData")) {
									HashMap gpsData = (HashMap) eventHashMap.get("GpsData");

									gps_time.append((String) gpsData.get("GpsTime"));
									server_time.append((String) gpsData.get("ServerTime"));
									device_time.append((String) eventHashMap.get("DeviceTime"));
									latitude.append((String) gpsData.get("Latitude"));
									longitude.append((String) gpsData.get("Longitude"));
									speed_accumulator.append((String) eventHashMap.get("SpeedAccumulator"));
									direction.append((String) eventHashMap.get("Direction"));
									speed.append((String) eventHashMap.get("Speed"));
									maximum_speed.append((String) eventHashMap.get("MaximumSpeed"));
									place_name.append((String) eventHashMap.get("PlaceName"));
									map_reference.append((String) eventHashMap.get("MapRef"));
									hdop.append((String) eventHashMap.get("Hdop"));
									num_satellites_in_view.append((String) eventHashMap.get("NumOfSatellitesInView"));
									altitude.append((String) eventHashMap.get("Altitude"));
								}

								if (eventHashMap.containsKey("Waypoint")) {
									HashMap waypoint = (HashMap) eventHashMap.get("Waypoint");

									set_point_id.append((String) waypoint.get("SetPointId"));
									set_point_name.append((String) waypoint.get("SetPointName"));
								}

								if (eventHashMap.containsKey("Address")) {
									HashMap address = (HashMap) eventHashMap.get("Address");

									address_number.append((String) address.get("Number"));
									address_street_name.append((String) address.get("StreetName"));
									address_suburb.append((String) address.get("Suburb"));
									address_city.append((String) address.get("City"));
									address_state.append((String) address.get("State"));
									address_postal_code.append((String) address.get("PostalCode"));
									address_country.append((String) address.get("Country"));
									address_timezone_offset.append((String) address.get("TimeZoneOffset"));
								}

								// Break Finished Early (no Job Data in this Report)
								if (report_id.toString().compareTo("2") == 0) {
									log.debug("Break Finished Early");

									try (
											PreparedStatement psInsert = connection.prepareStatement(Queries.eventBreakFinishedEarlySql);
									) {

										psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
										psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
										psInsert.setInt(3, Utils.tryParseInt(vehicle_id.toString(), 0));
										psInsert.setString(4, vehicle_name.toString());
										psInsert.setString(5, vehicle_external_reference.toString());
										psInsert.setInt(6, Utils.tryParseInt(driver_id.toString(), 0));
										psInsert.setString(7, driver_name.toString());
										psInsert.setString(8, driver_external_reference.toString());
										psInsert.setTimestamp(9, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
										psInsert.setTimestamp(10, new Timestamp(Utils.tryParseDate(server_time.toString(), defaultDate).getTime()));
										psInsert.setFloat(11, Utils.tryParseFloat(latitude.toString(), 0));
										psInsert.setFloat(12, Utils.tryParseFloat(longitude.toString(), 0));
										psInsert.setBigDecimal(13, new BigDecimal(eventSequence));

										psInsert.execute();
									}
									catch (SQLIntegrityConstraintViolationException sicve) {
										// NOP
									}
									catch (SQLServerException sqle) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error(sqle.getMessage());
										log.error(sqle.getSQLState());
									}
									catch (Exception e) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error("Error:", e);
									}

									continueProcessing = false;
								}

								// Missed Break Report (no Job Data in this Report)
								if (report_id.toString().compareTo("3") == 0) {
									log.debug("Break Missed");

									try (
											PreparedStatement psInsert = connection.prepareStatement(Queries.eventBreakMissedSql);
									) {

										psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
										psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
										psInsert.setInt(3, Utils.tryParseInt(vehicle_id.toString(), 0));
										psInsert.setString(4, vehicle_name.toString());
										psInsert.setString(5, vehicle_external_reference.toString());
										psInsert.setInt(6, Utils.tryParseInt(driver_id.toString(), 0));
										psInsert.setString(7, driver_name.toString());
										psInsert.setString(8, driver_external_reference.toString());
										psInsert.setTimestamp(9, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
										psInsert.setTimestamp(10, new Timestamp(Utils.tryParseDate(server_time.toString(), defaultDate).getTime()));
										psInsert.setFloat(11, Utils.tryParseFloat(latitude.toString(), 0));
										psInsert.setFloat(12, Utils.tryParseFloat(longitude.toString(), 0));
										psInsert.setBigDecimal(13, new BigDecimal(eventSequence));

										psInsert.execute();
									}
									catch (SQLIntegrityConstraintViolationException sicve) {
										// NOP
									}
									catch (SQLServerException sqle) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error(sqle.getMessage());
										log.error(sqle.getSQLState());
									}
									catch (Exception e) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error("Error:", e);
									}

									continueProcessing = false;
								}

								// Break Off (no Job Data in this Report)
								if (report_id.toString().compareTo("4") == 0) {
									log.debug("Break Off");

									try (
											PreparedStatement psInsert = connection.prepareStatement(Queries.eventBreakOffSql);
									) {

										psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
										psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
										psInsert.setInt(3, Utils.tryParseInt(vehicle_id.toString(), 0));
										psInsert.setString(4, vehicle_name.toString());
										psInsert.setString(5, vehicle_external_reference.toString());
										psInsert.setInt(6, Utils.tryParseInt(driver_id.toString(), 0));
										psInsert.setString(7, driver_name.toString());
										psInsert.setString(8, driver_external_reference.toString());
										psInsert.setTimestamp(9, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
										psInsert.setTimestamp(10, new Timestamp(Utils.tryParseDate(server_time.toString(), defaultDate).getTime()));
										psInsert.setFloat(11, Utils.tryParseFloat(latitude.toString(), 0));
										psInsert.setFloat(12, Utils.tryParseFloat(longitude.toString(), 0));
										psInsert.setBigDecimal(13, new BigDecimal(eventSequence));

										psInsert.execute();
									}
									catch (SQLIntegrityConstraintViolationException sicve) {
										// NOP
									}
									catch (SQLServerException sqle) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error(sqle.getMessage());
										log.error(sqle.getSQLState());
									}
									catch (Exception e) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error("Error:", e);
									}

									continueProcessing = false;
								}

								// Break On (no Job Data in this Report)
								if (report_id.toString().compareTo("5") == 0) {
									log.debug("Break On");

									try (
											PreparedStatement psInsert = connection.prepareStatement(Queries.eventBreakOnSql);
									) {

										psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
										psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
										psInsert.setInt(3, Utils.tryParseInt(vehicle_id.toString(), 0));
										psInsert.setString(4, vehicle_name.toString());
										psInsert.setString(5, vehicle_external_reference.toString());
										psInsert.setInt(6, Utils.tryParseInt(driver_id.toString(), 0));
										psInsert.setString(7, driver_name.toString());
										psInsert.setString(8, driver_external_reference.toString());
										psInsert.setTimestamp(9, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
										psInsert.setTimestamp(10, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
										psInsert.setFloat(11, Utils.tryParseFloat(latitude.toString(), 0));
										psInsert.setFloat(12, Utils.tryParseFloat(longitude.toString(), 0));
										psInsert.setBigDecimal(13, new BigDecimal(eventSequence));

										psInsert.execute();
									}
									catch (SQLIntegrityConstraintViolationException sicve) {
										// NOP
									}
									catch (SQLServerException sqle) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error(sqle.getMessage());
										log.error(sqle.getSQLState());
									}
									catch (Exception e) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error("Error:", e);
									}
									continueProcessing = false;
								}

								// Process Login Report (no Job Data in this Report)
								if (report_id.toString().compareTo("6") == 0) {
									log.debug("Login");

									String loginSql =
											"INSERT INTO [MTDATA_KINESIS].[dbo].events_login" +
													"(" +
													"     report_id" +
													"    ,fleet_id" +
													"    ,vehicle_id" +
													"    ,vehicle_name" +
													"    ,vehicle_external_reference" +
													"    ,driver_id" +
													"    ,driver_name" +
													"    ,driver_external_reference" +
													"    ,gps_time" +
													"    ,server_time" +
													"    ,latitude" +
													"    ,longitude" +
													"    ,odometer" +
													"    ,total_fuel" +
													"    ,sequence_id" +
													")" +
													"VALUES" +
													"(" +
													"       ?" +            // report_id
													"      ,?" +            // fleet_id
													"      ,?" +            // vehicle_id
													"      ,?" +            // vehicle_name
													"      ,?" +            // vehicle_external_reference
													"      ,?" +            // driver_id
													"      ,?" +            // driver_name
													"      ,?" +            // driver_external_reference
													"      ,?" +            // gps_time
													"      ,?" +            // server_time
													"      ,?" +            // latitude
													"      ,?" +            // longitude
													"      ,?" +            // odometer
													"      ,?" +            // total_fuel
													"      ,?" +            // sequence_id
													") ";

									continueProcessing = false;
								}

								// Process Logoff Report (no Job Data in this Report)
								if (report_id.toString().compareTo("7") == 0) {
									log.debug("Logoff");

									String logoffSql =
											"INSERT INTO [MTDATA_KINESIS].[dbo].events_logoff" +
													"(" +
													"     report_id" +
													"    ,fleet_id" +
													"    ,vehicle_id" +
													"    ,vehicle_name" +
													"    ,vehicle_external_reference" +
													"    ,driver_id" +
													"    ,driver_name" +
													"    ,driver_external_reference" +
													"    ,gps_time" +
													"    ,server_time" +
													"    ,latitude" +
													"    ,longitude" +
													"    ,odometer" +
													"    ,total_fuel" +
													"    ,sequence_id" +
													")" +
													"VALUES" +
													"(" +
													"       ?" +            // report_id
													"      ,?" +            // fleet_id
													"      ,?" +            // vehicle_id
													"      ,?" +            // vehicle_name
													"      ,?" +            // vehicle_external_reference
													"      ,?" +            // driver_id
													"      ,?" +            // driver_name
													"      ,?" +            // driver_external_reference
													"      ,?" +            // gps_time
													"      ,?" +            // server_time
													"      ,?" +            // latitude
													"      ,?" +            // longitude
													"      ,?" +            // odometer
													"      ,?" +            // total_fuel
													"      ,?" +            // sequence_id
													") ";

									try (
											PreparedStatement psInsert = connection.prepareStatement(logoffSql);
									) {

										psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
										psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
										psInsert.setInt(3, Utils.tryParseInt(vehicle_id.toString(), 0));
										psInsert.setString(4, vehicle_name.toString());
										psInsert.setString(5, vehicle_external_reference.toString());
										psInsert.setInt(6, Utils.tryParseInt(driver_id.toString(), 0));
										psInsert.setString(7, driver_name.toString());
										psInsert.setString(8, driver_external_reference.toString());
										psInsert.setTimestamp(9, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
										psInsert.setTimestamp(10, new Timestamp(Utils.tryParseDate(server_time.toString(), defaultDate).getTime()));
										psInsert.setFloat(11, Float.parseFloat(latitude.toString()));
										psInsert.setFloat(12, Float.parseFloat(longitude.toString()));
										psInsert.setInt(13, Utils.tryParseInt(odometer.toString(), 0));
										psInsert.setFloat(14, Float.parseFloat(total_fuel.toString()));
										psInsert.setBigDecimal(15, new BigDecimal(eventSequence));

										psInsert.execute();

									}
									catch (SQLIntegrityConstraintViolationException sicve) {
										// NOP
									}
									catch (SQLServerException sqle) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error(sqle.getMessage());
										log.error(sqle.getSQLState());
									}
									catch (Exception e) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error("Error:", e);
									}

									continueProcessing = false;
								}

								// Process Arrive Site Report (Job Data in this Report, but we want only Dispatch, Leg and Event ID's)
								if (report_id.toString().compareTo("8") == 0) {
									log.debug("Site Arrive");

									if (eventHashMap.containsKey("JobData")) {
										jobData = (HashMap) eventHashMap.get("JobData");
										dispatch_id.append((String) jobData.get("DispatchId"));
										leg_id.append((String) jobData.get("LegId"));
										event_id.append((String) jobData.get("JobEventId"));
										job_status.append((String) jobData.get("JobStatus"));
									}

									try (
											PreparedStatement psInsert = connection.prepareStatement(Queries.eventSiteArriveSql);
									) {

										psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
										psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
										psInsert.setString(3, fleet_name.toString());
										psInsert.setInt(4, Utils.tryParseInt(vehicle_id.toString(), 0));
										psInsert.setString(5, vehicle_name.toString());
										psInsert.setString(6, vehicle_external_reference.toString());
										psInsert.setInt(7, Utils.tryParseInt(driver_id.toString(), 0));
										psInsert.setString(8, driver_name.toString());
										psInsert.setString(9, driver_external_reference.toString());
										psInsert.setInt(10, Utils.tryParseInt(dispatch_id.toString(), 0));
										psInsert.setInt(11, Utils.tryParseInt(leg_id.toString(), 0));
										psInsert.setInt(12, Utils.tryParseInt(event_id.toString(), 0));
										psInsert.setInt(13, Utils.tryParseInt(job_status.toString(), 0));
										psInsert.setTimestamp(14, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
										psInsert.setTimestamp(15, new Timestamp(Utils.tryParseDate(server_time.toString(), defaultDate).getTime()));
										psInsert.setFloat(16, Utils.tryParseFloat(latitude.toString(), 0));
										psInsert.setFloat(17, Utils.tryParseFloat(longitude.toString(), 0));
										psInsert.setInt(18, Utils.tryParseInt(odometer.toString(), 0));
										psInsert.setFloat(19, Utils.tryParseFloat(total_fuel.toString(), 0));
										psInsert.setString(20, address_number.toString());
										psInsert.setString(21, address_street_name.toString());
										psInsert.setString(22, address_suburb.toString());
										psInsert.setString(23, address_city.toString());
										psInsert.setString(24, address_state.toString());
										psInsert.setString(25, address_postal_code.toString());
										psInsert.setString(26, address_country.toString());
										psInsert.setString(27, address_timezone_offset.toString());
										psInsert.setBigDecimal(28, new BigDecimal(eventSequence));
										psInsert.execute();

									}
									catch (SQLIntegrityConstraintViolationException sicve) {
										// NOP
									}
									catch (SQLServerException sqle) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error(sqle.getMessage());
										log.error(sqle.getSQLState());
									}
									catch (Exception e) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error("Error:", e);
									}

									continueProcessing = false;
								}

								// Process Depart Site Report (Job Data in this Report, but we want only Dispatch, Leg and Event ID's)
								if (report_id.toString().compareTo("12") == 0) {
									log.debug("Site Depart");

									if (eventHashMap.containsKey("JobData")) {
										jobData = (HashMap) eventHashMap.get("JobData");
										dispatch_id.append((String) jobData.get("DispatchId"));
										leg_id.append((String) jobData.get("LegId"));
										event_id.append((String) jobData.get("JobEventId"));
										job_status.append((String) jobData.get("JobStatusAtTimeOfUpdate"));
									}

									try (
											PreparedStatement psInsert = connection.prepareStatement(Queries.eventSiteDepartSql);
									) {

										psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
										psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
										psInsert.setString(3, fleet_name.toString());
										psInsert.setInt(4, Utils.tryParseInt(vehicle_id.toString(), 0));
										psInsert.setString(5, vehicle_name.toString());
										psInsert.setString(6, vehicle_external_reference.toString());
										psInsert.setInt(7, Utils.tryParseInt(driver_id.toString(), 0));
										psInsert.setString(8, driver_name.toString());
										psInsert.setString(9, driver_external_reference.toString());
										psInsert.setInt(10, Utils.tryParseInt(dispatch_id.toString(), 0));
										psInsert.setInt(11, Utils.tryParseInt(leg_id.toString(), 0));
										psInsert.setInt(12, Utils.tryParseInt(event_id.toString(), 0));
										psInsert.setInt(13, Utils.tryParseInt(job_status.toString(), 0));
										psInsert.setTimestamp(14, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
										psInsert.setTimestamp(15, new Timestamp(Utils.tryParseDate(server_time.toString(), defaultDate).getTime()));
										psInsert.setFloat(16, Utils.tryParseFloat(latitude.toString(), 0));
										psInsert.setFloat(17, Utils.tryParseFloat(longitude.toString(), 0));
										psInsert.setInt(18, Utils.tryParseInt(odometer.toString(), 0));
										psInsert.setFloat(19, Utils.tryParseFloat(total_fuel.toString(), 0));
										psInsert.setString(20, address_number.toString());
										psInsert.setString(21, address_street_name.toString());
										psInsert.setString(22, address_suburb.toString());
										psInsert.setString(23, address_city.toString());
										psInsert.setString(24, address_state.toString());
										psInsert.setString(25, address_postal_code.toString());
										psInsert.setString(26, address_country.toString());
										psInsert.setString(27, address_timezone_offset.toString());
										psInsert.setBigDecimal(28, new BigDecimal(eventSequence));
										psInsert.execute();

									}
									catch (SQLIntegrityConstraintViolationException sicve) {
										// NOP
									}
									catch (SQLServerException sqle) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error(sqle.getMessage());
										log.error(sqle.getSQLState());
									}
									catch (Exception e) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error("Error:", e);
									}

									continueProcessing = false;
								}

								// task no 14 -- Job Action Complete

								if(report_id.toString().compareTo("14") == 0){

									log.debug("Job Action Complete");

									if (eventHashMap.containsKey("JobData")) {
										jobData = (HashMap) eventHashMap.get("JobData");
										dispatch_id.append((String) jobData.get("DispatchId"));
										leg_id.append((String) jobData.get("LegId"));
										event_id.append((String) jobData.get("JobEventId"));
										job_status.append((String) jobData.get("JobStatus"));
									}

									try (
											PreparedStatement psInsert = connection.prepareStatement(jobActionCompleteSql);
									) {

										psInsert.setInt(1, Utils.tryParseInt(fleet_id.toString(), 0));
										psInsert.setString(2, fleet_name.toString());
										psInsert.setInt(3, Utils.tryParseInt(vehicle_id.toString(), 0));
										psInsert.setString(4, vehicle_name.toString());
										psInsert.setString(5, vehicle_external_reference.toString());
										psInsert.setInt(6, Utils.tryParseInt(driver_id.toString(), 0));
										psInsert.setString(7, driver_name.toString());
										psInsert.setString(8,driver_external_reference.toString());
//
										psInsert.setInt(9, Utils.tryParseInt(report_id.toString(),0));

										psInsert.setInt(10, Utils.tryParseInt(odometer.toString(), 0));
										psInsert.setInt(11, Utils.tryParseInt(total_fuel.toString(),0));

//										psInsert.setInt(12, Utils.tryParseInt(report_id.toString(), 0));

//										psInsert.setString(13, vehicle_external_reference.toString());

//										psInsert.setString(8, driver_external_reference.toString());
//										psInsert.setTimestamp(9, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
//										psInsert.setTimestamp(10, new Timestamp(Utils.tryParseDate(server_time.toString(), defaultDate).getTime()));
//										psInsert.setString(20, address_number.toString());
//										psInsert.setString(21, address_street_name.toString());
//										psInsert.setString(22, address_suburb.toString());
//										psInsert.setString(23, address_city.toString());
//										psInsert.setString(24, address_state.toString());
//										psInsert.setString(25, address_postal_code.toString());
//										psInsert.setString(26, address_country.toString());
//										psInsert.setString(27, address_timezone_offset.toString());
//										psInsert.setFloat(11, Float.parseFloat(latitude.toString()));
//										psInsert.setFloat(12, Float.parseFloat(longitude.toString()));

//										psInsert.setBigDecimal(15, new BigDecimal(eventSequence));



										psInsert.execute();

									}	catch (Exception e) {
										log.error("Error Processing Event Sequence - " + eventSequence.toString());
										log.error("Error:", e);
									}

									continueProcessing = false;

								}

								result = eventHashMap.containsKey("JobData");
								if (eventHashMap.containsKey("JobData") && continueProcessing == true) {

									jobData = (HashMap) eventHashMap.get("JobData");
									dispatch_id.append((String) jobData.get("DispatchId"));
									report_leg_id.append((String) jobData.get("LegId"));

									if (report_leg_id == null) report_leg_id.setLength(0);

									// Move to nested JobData node
									if (jobData.containsKey("JobData")) {

										jobData = (HashMap) jobData.get("JobData");

										job_reference.append((String) jobData.get("JobReference"));
										job_reference_int.append((String) jobData.get("JobReferenceInt"));
										job_title.append((String) jobData.get("Title"));
										docket_number.append((String) jobData.get("DocketNumber"));
										job_type.append((String) jobData.get("JobType"));
										job_status.append((String) jobData.get("JobStatus"));

										StringBuilder jobCompleteLegId = null;

										// Process Job Status Report
										if (report_id.toString().compareTo("20") == 0) {

											log.debug("Job Status Report");

											// Get Leg Data
											if (jobData.containsKey("LegData")) {

												Object nullTest = jobData.get("LegData");

												if (nullTest.toString().compareTo("[]") != 0) {

													legData = (ArrayList) jobData.get("LegData");

													StringBuilder finalLatitude = latitude;
													StringBuilder finalGps_time = gps_time;
													StringBuilder finalServer_time = server_time;
													StringBuilder finalLongitude = longitude;
													StringBuilder finalDispatch_id = dispatch_id;
													BigInteger finalEventSequence = eventSequence;
													StringBuilder finalJob_reference = job_reference;

													StringBuilder finalDocket_number = docket_number;
													StringBuilder finalJob_reference_int = job_reference_int;
													StringBuilder finalReport_leg_id = report_leg_id;
													StringBuilder finalJob_status = job_status;
													StringBuilder finalJob_type = job_type;
													HashMap finalJobData = jobData;
													StringBuilder finalJobCompleteLegId = jobCompleteLegId;

													legData.forEach((legDataObj) -> {


														HashMap legDataElement = (HashMap) legDataObj;

														StringBuilder leg_LegVersion = Utils.safeStringToStringBuilder((String) legDataElement.get("Version"));
														StringBuilder leg_LegId = Utils.safeStringToStringBuilder((String) legDataElement.get("LegId"));
														StringBuilder leg_LegType = Utils.safeStringToStringBuilder((String) legDataElement.get("LegType"));
														StringBuilder leg_LegStatus = Utils.safeStringToStringBuilder((String) legDataElement.get("LegStatus"));
														StringBuilder leg_LegNumber = Utils.safeStringToStringBuilder((String) legDataElement.get("LegNumber"));
														StringBuilder leg_Title = Utils.safeStringToStringBuilder((String) legDataElement.get("Title"));
														StringBuilder leg_CustomerGroupId = Utils.safeStringToStringBuilder((String) legDataElement.get("CustomerGroupId"));
														StringBuilder leg_CustomerGroupVersion = Utils.safeStringToStringBuilder((String) legDataElement.get("CustomerGroupVersion"));
														StringBuilder leg_CustomerId = Utils.safeStringToStringBuilder((String) legDataElement.get("CustomerId"));
														StringBuilder leg_LocationName = Utils.safeStringToStringBuilder((String) legDataElement.get("LocationName"));
														StringBuilder leg_Address = Utils.safeStringToStringBuilder((String) legDataElement.get("Address"));
														StringBuilder leg_Latitude = Utils.safeStringToStringBuilder((String) legDataElement.get("Latitude"));
														StringBuilder leg_Longitude = Utils.safeStringToStringBuilder((String) legDataElement.get("Longitude"));
														StringBuilder leg_Radius = Utils.safeStringToStringBuilder((String) legDataElement.get("Radius"));
														StringBuilder leg_JobReferenceType = new StringBuilder();

														// Identify type of job reference

														Matcher matcherJobReferenceWaveId = patternJobReferenceWaveId.matcher(finalJob_reference);
														if (matcherJobReferenceWaveId.find()) {
															leg_JobReferenceType.append("WAVE");
														} else {
															Matcher matcherJobReferenceSONumber = patternJobReferenceSONumber.matcher(finalJob_reference);
															if (matcherJobReferenceSONumber.find()) {
																leg_JobReferenceType.append("SO");
															} else leg_JobReferenceType.append("NONE");

														}

														try (
																PreparedStatement psInsert = connection.prepareStatement(Queries.legStatusSql);
														) {

															psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
															psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
															psInsert.setInt(3, Utils.tryParseInt(vehicle_id.toString(), 0));
															psInsert.setString(4, vehicle_name.toString());
															psInsert.setInt(5, Utils.tryParseInt(driver_id.toString(), 0));
															psInsert.setString(6, driver_name.toString());
															psInsert.setString(7, driver_external_reference.toString());
															psInsert.setTimestamp(8, new Timestamp(Utils.tryParseDate(finalGps_time.toString(), defaultDate).getTime()));
															psInsert.setTimestamp(9, new Timestamp(Utils.tryParseDate(finalServer_time.toString(), defaultDate).getTime()));
															psInsert.setFloat(10, Utils.tryParseFloat(finalLatitude.toString(), 0));
															psInsert.setFloat(11, Utils.tryParseFloat(finalLongitude.toString(), 0));
															psInsert.setInt(12, Utils.tryParseInt(finalDispatch_id.toString(), 0));
															psInsert.setString(13, finalJob_reference.toString());
															psInsert.setString(14, leg_JobReferenceType.toString());
															psInsert.setInt(15, Utils.tryParseInt(finalJob_reference_int.toString(), 0));
															psInsert.setInt(16, Utils.tryParseInt(finalJob_type.toString(), 0));
															psInsert.setInt(17, Utils.tryParseInt(finalJob_status.toString(), 0));
															psInsert.setString(18, finalDocket_number.toString());
															psInsert.setInt(19, Utils.tryParseInt(leg_LegVersion.toString(), 0));
															psInsert.setInt(20, Utils.tryParseInt(leg_LegId.toString(), 0));
															psInsert.setInt(21, Utils.tryParseInt(leg_LegType.toString(), 0));
															psInsert.setInt(22, Utils.tryParseInt(leg_LegStatus.toString(), 0));
															psInsert.setInt(23, Utils.tryParseInt(leg_LegNumber.toString(), 0));
															if(leg_Title == null) { psInsert.setNull(24, Types.NVARCHAR); } else { psInsert.setString(24, leg_Title.toString()); }
															psInsert.setInt(25, Utils.tryParseInt(leg_CustomerGroupId.toString(), 0));
															psInsert.setInt(26, Utils.tryParseInt(leg_CustomerGroupVersion.toString(), 0));
															psInsert.setInt(27, Utils.tryParseInt(leg_CustomerId.toString(), 0));
															if(leg_LocationName == null) { psInsert.setNull(28, Types.NVARCHAR); } else { psInsert.setString(28, leg_LocationName.toString()); }
															if(leg_Address == null) { psInsert.setNull(29, Types.NVARCHAR); } else { psInsert.setString(29, leg_Address.toString()); }
															psInsert.setFloat(30, Utils.tryParseFloat(leg_Latitude.toString(), 0));
															psInsert.setFloat(31, Utils.tryParseFloat(leg_Longitude.toString(), 0));
															psInsert.setInt(32, Utils.tryParseInt(leg_Radius.toString(), 0));
															psInsert.setBigDecimal(33, new BigDecimal(finalEventSequence));

															psInsert.execute();
														}
														catch (SQLIntegrityConstraintViolationException sicve) {
															// NOP
														}
														catch (SQLServerException sqle) {
															log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
															log.error(sqle.getMessage());
															log.error(sqle.getSQLState());
														}
														catch (Exception e) {
															log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
															log.error("Error:", e);
														}

													});

												}

											}

											continueProcessing = false;
										}

										// Process Start Actions Report
										if (report_id.toString().compareTo("24") == 0) {
											log.debug("Start Actions Report");

											// Get Leg Data
											if (jobData.containsKey("LegData")) {

												Object nullTest = jobData.get("LegData");

												// Leg data is not empty
												if (nullTest.toString().compareTo("[]") != 0) {


													HashMap finalJobData = jobData;

													// Iterate through legs to find the leg that this event refers to

													AtomicReference<String> leg_LegId = new AtomicReference<>("");
													AtomicReference<String> leg_LegType = new AtomicReference<>("");
													AtomicReference<String> leg_LegStatus = new AtomicReference<>("");
													AtomicReference<String> leg_LegNumber = new AtomicReference<>("");
													AtomicReference<String> leg_LegTitle = new AtomicReference<>("");

													AtomicReference<String> leg_LegActionType = new AtomicReference<>("");
													AtomicReference<String> leg_LegActionId = new AtomicReference<>("");
													AtomicReference<String> leg_LegActionStatus = new AtomicReference<>("");
													AtomicReference<String> leg_LegActionNumber = new AtomicReference<>("");

													legData = (ArrayList) jobData.get("LegData");

													StringBuilder finalReport_leg_id = report_leg_id;

													legData.forEach((legDataObj) -> {
														HashMap legDataElement = (HashMap) legDataObj;

														if (((String) legDataElement.get("LegId")).compareTo(finalReport_leg_id.toString()) == 0) {
															leg_LegId.set((String) legDataElement.get("LegId"));
															leg_LegType.set((String) legDataElement.get("LegType"));
															leg_LegStatus.set((String) legDataElement.get("LegStatus"));
															leg_LegNumber.set((String) legDataElement.get("LegNumber"));
															leg_LegTitle.set((String) legDataElement.get("Title"));

															// Leg Actions
															if (legDataElement.containsKey("LegActionData")) {

																ArrayList legActionData = (ArrayList) legDataElement.get("LegActionData");


																legActionData.forEach((legActionDataObj) -> {

																	HashMap legActionDataElement = (HashMap) legActionDataObj;

																	if (((String) legActionDataElement.get("LegActionNumber")).compareTo("1") == 0) {

																		leg_LegActionType.set((String) legActionDataElement.get("LegActionType"));
																		// StringBuilder legActionType =   new StringBuilder((String) legActionDataElement.get("LegActionType"));

																		leg_LegActionId.set((String) legActionDataElement.get("LegActionId"));
																		// StringBuilder legActionId = new StringBuilder((String) legActionDataElement.get("LegActionId"));

																		leg_LegActionStatus.set((String) legActionDataElement.get("LegActionStatus"));
																		// StringBuilder legActionStatus = new StringBuilder((String) legActionDataElement.get("LegActionStatus"));

																		leg_LegActionNumber.set((String) legActionDataElement.get("LegActionNumber"));
																		// StringBuilder legActionNumber = new StringBuilder((String) legActionDataElement.get("LegActionNumber"));

																	}



																});

															}

														}
													});

													try (
															PreparedStatement psInsert = connection.prepareStatement(Queries.legStartActionsSql);
													) {

														psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
														psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
														psInsert.setInt(3, Utils.tryParseInt(vehicle_id.toString(), 0));
														psInsert.setString(4, vehicle_name.toString());
														psInsert.setInt(5, Utils.tryParseInt(driver_id.toString(), 0));
														psInsert.setString(6, driver_name.toString());
														psInsert.setString(7, driver_external_reference.toString());
														psInsert.setTimestamp(8, new Timestamp(Utils.tryParseDate(gps_time.toString(), defaultDate).getTime()));
														psInsert.setTimestamp(9, new Timestamp(Utils.tryParseDate(server_time.toString(), defaultDate).getTime()));
														psInsert.setFloat(10, Utils.tryParseFloat(latitude.toString(), 0));
														psInsert.setFloat(11, Utils.tryParseFloat(longitude.toString(), 0));
														psInsert.setInt(12, Utils.tryParseInt(dispatch_id.toString(), 0));
														psInsert.setInt(13, Utils.tryParseInt(job_status.toString(), 0));


														psInsert.setInt(14, Utils.tryParseInt(leg_LegId.get(), 0));
														psInsert.setInt(15, Utils.tryParseInt(leg_LegNumber.get(), 0));
														psInsert.setInt(16, Utils.tryParseInt(leg_LegType.get(), 0));
														psInsert.setInt(17, Utils.tryParseInt(leg_LegStatus.get(), 0));
														psInsert.setString(18, leg_LegTitle.get());


														psInsert.setInt(19, Utils.tryParseInt(leg_LegActionType.get(), 0));
														psInsert.setInt(20, Utils.tryParseInt(leg_LegActionId.get(), 0));
														psInsert.setInt(21, Utils.tryParseInt(leg_LegActionStatus.get(), 0));
														psInsert.setInt(22, Utils.tryParseInt(leg_LegActionNumber.get(), 0));

														psInsert.setBigDecimal(23, new BigDecimal(eventSequence));

														psInsert.execute();

													}
													catch (SQLIntegrityConstraintViolationException sicve) {
														// NOP
													}
													catch (SQLServerException sqle) {
														log.error("Error Processing Event Sequence - " + eventSequence.toString());
														log.error(sqle.getMessage());
														log.error(sqle.getSQLState());
													}
													catch (Exception e) {
														log.error("Error Processing Event Sequence - " + eventSequence.toString());
														log.error("Error:", e);
													}
												}
											}

											continueProcessing = false;

										}

										// If this is a job complete it is processed as an implicit leg complete for the last leg of the job
										// An enhancement has been requested from MTData for this.  Until the final leg complete event is emitted
										// it is necessary to loop through the job events to find the last reference leg base on GPSTime.  This
										// is then the completed leg
										if (report_id.toString().compareTo("17") == 0 && continueProcessing == true) {
											log.debug("Job Complete with Leg");

											// Parse Job Action Data
											ArrayList jobEventData;
											if (jobData.containsKey("JobEventData")) {

												jobEventData = (ArrayList) jobData.get("JobEventData");

												Collections.sort(jobEventData, new JobEventDataComparator());

												Collections.reverse(jobEventData);

												Iterator iterJobEventData = jobEventData.iterator();
												while (iterJobEventData.hasNext()) {
													;

													HashMap jobEventDataElement = (HashMap) iterJobEventData.next();

													StringBuilder leg_Number = new StringBuilder((String) jobEventDataElement.get("LegId"));

													if (leg_Number.toString().compareTo("-1") != 0) {

														jobCompleteLegId = leg_Number;

														// Sorted list, the first match should be the leg id we seek
														break;

													}


												}

											}

										}

										// Get Leg Data
										if (jobData.containsKey("LegData") && continueProcessing == true) {

											Object nullTest = jobData.get("LegData");

											if (nullTest.toString().compareTo("[]") != 0) {

												//if (jobData.get("LegData").toString().compareTo("") != 0) {

												legData = (ArrayList) jobData.get("LegData");

												StringBuilder finalLatitude = latitude;
												StringBuilder finalGps_time = gps_time;
												StringBuilder finalServer_time = server_time;
												StringBuilder finalLongitude = longitude;
												StringBuilder finalDispatch_id = dispatch_id;
												BigInteger finalEventSequence = eventSequence;
												StringBuilder finalJob_reference = job_reference;

												StringBuilder finalDocket_number = docket_number;
												StringBuilder finalJob_reference_int = job_reference_int;
												StringBuilder finalJob_title = job_title;
												StringBuilder finalReport_leg_id = report_leg_id;
												StringBuilder finalJob_status = job_status;
												StringBuilder finalJob_type = job_type;
												HashMap finalJobData = jobData;
												StringBuilder finalJobCompleteLegId = jobCompleteLegId;

												legData.forEach((legDataObj) -> {

													HashMap legDataElement = (HashMap) legDataObj;

													StringBuilder leg_LegVersion = new StringBuilder(nullToEmpty((String) legDataElement.get("Version")));
													StringBuilder leg_LegId = new StringBuilder(nullToEmpty((String) legDataElement.get("LegId")));
													StringBuilder leg_LegType = new StringBuilder(nullToEmpty((String) legDataElement.get("LegType")));
													StringBuilder leg_LegStatus = new StringBuilder(nullToEmpty((String) legDataElement.get("LegStatus")));
													StringBuilder leg_LegNumber = new StringBuilder(nullToEmpty((String) legDataElement.get("LegNumber")));
													StringBuilder leg_LegExternalRef = new StringBuilder(nullToEmpty((String) legDataElement.get("ExternalReference")));
													StringBuilder leg_Title = new StringBuilder(nullToEmpty((String) legDataElement.get("Title")));
													StringBuilder leg_CustomerGroupId = new StringBuilder(nullToEmpty((String) legDataElement.get("CustomerGroupId")));
													StringBuilder leg_CustomerGroupVersion = new StringBuilder(nullToEmpty((String) legDataElement.get("CustomerGroupVersion")));
													StringBuilder leg_CustomerId = new StringBuilder(nullToEmpty((String) legDataElement.get("CustomerId")));
													StringBuilder leg_CustomerExternalRef = new StringBuilder(nullToEmpty((String) legDataElement.get("CustomerExternalRef")));
													StringBuilder leg_LocationName = new StringBuilder(nullToEmpty((String) legDataElement.get("LocationName")));
													StringBuilder leg_Address = new StringBuilder(nullToEmpty((String) legDataElement.get("Address")));
													StringBuilder leg_Latitude = new StringBuilder(nullToEmpty((String) legDataElement.get("Latitude")));
													StringBuilder leg_Longitude = new StringBuilder(nullToEmpty((String) legDataElement.get("Longitude")));
													StringBuilder leg_Radius = new StringBuilder(nullToEmpty((String) legDataElement.get("Radius")));
													StringBuilder leg_ArriveTime = new StringBuilder(nullToEmpty((String) legDataElement.get("ArriveTime")));
													StringBuilder leg_ArriveTimeTolerance = new StringBuilder(nullToEmpty((String) legDataElement.get("ArriveTimeTolerance")));
													StringBuilder leg_DepartTime = new StringBuilder(nullToEmpty((String) legDataElement.get("DepartTime")));
													StringBuilder leg_DepartTimeTolerance = new StringBuilder(nullToEmpty((String) legDataElement.get("DepartTimeTolerance")));
													StringBuilder leg_JobReferenceType = new StringBuilder();

													// Identify type of job reference

													Matcher matcherJobReferenceWaveId = patternJobReferenceWaveId.matcher(finalJob_reference);
													if (matcherJobReferenceWaveId.find()) {
														leg_JobReferenceType.append("WAVE");
													} else {
														Matcher matcherJobReferenceSONumber = patternJobReferenceSONumber.matcher(finalJob_reference);
														if (matcherJobReferenceSONumber.find()) {
															leg_JobReferenceType.append("SO");
														} else leg_JobReferenceType.append("NONE");

													}

													// Completed Legs
													if ((report_id.toString().compareTo("23") == 0
															&& (leg_LegId.compareTo(finalReport_leg_id) == 0)
													)
															|| (report_id.toString().compareTo("17") == 0
															&& leg_LegId.compareTo(finalJobCompleteLegId) == 0

													) 		|| (report_id.toString().compareTo("14") == 0)
															&& leg_LegId.compareTo(finalJobCompleteLegId) == 0
													) {

														if (report_id.toString().compareTo("14") == 0) {
															log.debug("Job Complete");
														}


														try (
																PreparedStatement psInsert = connection.prepareStatement(Queries.jobActionCompleteSql);
														) {

															psInsert.setInt(1, Utils.tryParseInt(report_id.toString(), 0));
															psInsert.setInt(2, Utils.tryParseInt(fleet_id.toString(), 0));
															psInsert.setInt(3, Utils.tryParseInt(vehicle_id.toString(), 0));
															psInsert.setString(4, vehicle_name.toString());
															psInsert.setString(5, vehicle_external_reference.toString());
															psInsert.setInt(6, Utils.tryParseInt(driver_id.toString(), 0));
															psInsert.setString(7, driver_name.toString());
															psInsert.setString(8, driver_external_reference.toString());
//															psInsert.setTimestamp(9, new Timestamp(Utils.tryParseDate(finalGps_time.toString(), defaultDate).getTime()));
//															psInsert.setTimestamp(10, new Timestamp(Utils.tryParseDate(finalServer_time.toString(), defaultDate).getTime()));
															psInsert.setFloat(11, Utils.tryParseFloat(finalLatitude.toString(), 0));
															psInsert.setFloat(12, Utils.tryParseFloat(finalLongitude.toString(), 0));
															psInsert.setInt(13, Utils.tryParseInt(finalDispatch_id.toString(), 0));
															psInsert.setString(14, finalJob_reference.toString());
															psInsert.setString(15, leg_JobReferenceType.toString());
															psInsert.setInt(16, Utils.tryParseInt(finalJob_reference_int.toString(), 0));
															psInsert.setInt(17, Utils.tryParseInt(finalJob_type.toString(), 0));
															psInsert.setInt(18, Utils.tryParseInt(finalJob_status.toString(), 0));
															psInsert.setString(19, finalJob_title.toString());
															psInsert.setString(20, finalDocket_number.toString());
															psInsert.setString(21, address_number.toString());
															psInsert.setString(22, address_street_name.toString());
															psInsert.setString(23, address_suburb.toString());
															psInsert.setString(24, address_city.toString());
															psInsert.setString(25, address_state.toString());
															psInsert.setString(26, address_postal_code.toString());
															psInsert.setString(27, address_country.toString());
															psInsert.setString(28, address_timezone_offset.toString());
															psInsert.setInt(29, Utils.tryParseInt(leg_LegVersion.toString(), 0));
															psInsert.setInt(30, Utils.tryParseInt(leg_LegId.toString(), 0));
															psInsert.setInt(31, Utils.tryParseInt(leg_LegType.toString(), 0));
															psInsert.setInt(32, Utils.tryParseInt(leg_LegStatus.toString(), 0));
															psInsert.setInt(33, Utils.tryParseInt(leg_LegNumber.toString(), 0));
															psInsert.setString(34, leg_LegExternalRef.toString());
															psInsert.setString(35, leg_Title.toString());
															psInsert.setInt(36, Utils.tryParseInt(leg_CustomerGroupId.toString(), 0));
															psInsert.setInt(37, Utils.tryParseInt(leg_CustomerGroupVersion.toString(), 0));
															psInsert.setInt(38, Utils.tryParseInt(leg_CustomerId.toString(), 0));
															psInsert.setString(39, leg_CustomerExternalRef.toString());
															psInsert.setString(40, leg_LocationName.toString());
															psInsert.setString(41, leg_Address.toString());
															psInsert.setFloat(42, Utils.tryParseFloat(leg_Latitude.toString(), 0));
															psInsert.setFloat(43, Utils.tryParseFloat(leg_Longitude.toString(), 0));
															psInsert.setInt(44, Utils.tryParseInt(leg_Radius.toString(), 0));
															psInsert.setTimestamp(45, new Timestamp(Utils.tryParseDate(leg_ArriveTime.toString(), defaultDate).getTime()));
															psInsert.setInt(46, Utils.tryParseInt(leg_ArriveTimeTolerance.toString(), 0));
															psInsert.setTimestamp(47, new Timestamp(Utils.tryParseDate(leg_DepartTime.toString(), defaultDate).getTime()));
															psInsert.setInt(48, Utils.tryParseInt(leg_DepartTimeTolerance.toString(), 0));
															psInsert.setBigDecimal(49, new BigDecimal(finalEventSequence));

															psInsert.execute();

														}
														catch (SQLIntegrityConstraintViolationException sicve) {
															// NOP
														}
														catch (SQLServerException sqle) {
															log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
															log.error(sqle.getMessage());
															log.error(sqle.getSQLState());
														}
														catch (Exception e) {
															log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
															log.error("Error:", e);
														}

														// Leg Actions
														if (legDataElement.containsKey("LegActionData")) {

															ArrayList legActionData = (ArrayList) legDataElement.get("LegActionData");


															legActionData.forEach((legActionDataObj) -> {

																HashMap legActionDataElement = (HashMap) legActionDataObj;
																StringBuilder legActionType =   new StringBuilder((String) legActionDataElement.get("LegActionType"));
																StringBuilder legActionId = new StringBuilder((String) legActionDataElement.get("LegActionId"));
																StringBuilder legActionStatus = new StringBuilder((String) legActionDataElement.get("LegActionStatus"));
																StringBuilder legActionNumber = new StringBuilder((String) legActionDataElement.get("LegActionNumber"));

																// Store Leg Items
																if (legActionDataElement.get("Items") != null && legActionDataElement.get("Items").getClass().getTypeName().compareTo("java.util.ArrayList") == 0) {

																	ArrayList legActionDataItems = (ArrayList) legActionDataElement.get("Items");

																	legActionDataItems.forEach((legActionDataItemsObj) -> {

																		HashMap legActionDataItemsElement = (HashMap) legActionDataItemsObj;

																		StringBuilder legActionsData_Item_Id = new StringBuilder(); legActionsData_Item_Id.append((String) legActionDataItemsElement.get("ItemId"));
																		StringBuilder legActionsData_Item_List_Id = new StringBuilder(); legActionsData_Item_List_Id.append((String) legActionDataItemsElement.get("ItemListId"));
																		StringBuilder legActionsData_Item_List_Version = new StringBuilder(); legActionsData_Item_List_Version.append((String) legActionDataItemsElement.get("ItemListVersion"));
																		StringBuilder legActionsData_Item_External_Reference = new StringBuilder(); legActionsData_Item_External_Reference.append((String) legActionDataItemsElement.get("ExternalReference"));
																		StringBuilder legActionsData_Item_Count = new StringBuilder(); legActionsData_Item_Count.append((String) legActionDataItemsElement.get("Count"));
																		StringBuilder legActionsData_Item_Has_Adjustment = new StringBuilder(); legActionsData_Item_Has_Adjustment.append((String) legActionDataItemsElement.get("HasAdjustment"));
																		StringBuilder legActionsData_Item_Seq_Id = new StringBuilder(); legActionsData_Item_Seq_Id.append((String) legActionDataItemsElement.get("Id"));
																		StringBuilder legActionsData_Item_Name = new StringBuilder(); legActionsData_Item_Name.append((String) legActionDataItemsElement.get("Name"));

																		if (legActionDataItemsElement != null) {

																			try (
																					PreparedStatement psInsert = connection.prepareStatement(Queries.legCompleteItemsSql);
																			) {

																				psInsert.setInt(1, Utils.tryParseInt(leg_LegVersion.toString(), 0));
																				psInsert.setInt(2, Utils.tryParseInt(leg_LegId.toString(), 0));
																				psInsert.setInt(3, Utils.tryParseInt(leg_LegType.toString(), 0));
																				psInsert.setInt(4, Utils.tryParseInt(leg_LegStatus.toString(), 0));
																				psInsert.setInt(5, Utils.tryParseInt(leg_LegNumber.toString(), 0));

																				psInsert.setInt(6, Utils.tryParseInt(legActionType.toString(), 0));
																				psInsert.setInt(7, Utils.tryParseInt(legActionId.toString(), 0));
																				psInsert.setInt(8, Utils.tryParseInt(legActionStatus.toString(), 0));
																				psInsert.setInt(9, Utils.tryParseInt(legActionNumber.toString(), 0));

																				psInsert.setInt(10, Utils.tryParseInt(legActionsData_Item_Id.toString(), 0));
																				psInsert.setInt(11, Utils.tryParseInt(legActionsData_Item_List_Id.toString(), 0));
																				psInsert.setInt(12, Utils.tryParseInt(legActionsData_Item_Seq_Id.toString(), 0));
																				psInsert.setInt(13, Utils.tryParseInt(legActionsData_Item_List_Version.toString(), 0));
																				psInsert.setString(14, legActionsData_Item_External_Reference.toString());
																				psInsert.setString(15, legActionsData_Item_Name.toString());
																				psInsert.setInt(16, Utils.tryParseInt(legActionsData_Item_Count.toString(), 0));

																				psInsert.setBigDecimal(17, new BigDecimal(finalEventSequence));

																				psInsert.execute();

																			}
																			catch (SQLIntegrityConstraintViolationException sicve) {
																				// NOP
																			}
																			catch (SQLServerException sqle) {
																				log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																				log.error(sqle.getMessage());
																				log.error(sqle.getSQLState());
																			}
																			catch (Exception e) {
																				log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																				log.error("Error:", e);
																			}
																		}

																	});

																}

																// Leg Action 0 - Q & A
																if (Integer.parseInt(legActionType.toString()) == 0) {

																	if (legActionDataElement.containsKey("CompletedLegActions")) {
																		HashMap legActionCompletedLegActions = (HashMap) legActionDataElement.get("CompletedLegActions");

																		if (legActionCompletedLegActions.containsKey("QuestionAnswers")) {

																			HashMap legActionCompletedLegActionsQuestionAnswers = (HashMap) legActionCompletedLegActions.get("QuestionAnswers");

																			StringBuilder legAction_QuestionListId = new StringBuilder(); legAction_QuestionListId.append((String) legActionCompletedLegActionsQuestionAnswers.get("QuestionListId"));
																			StringBuilder legAction_QuestionListVersion = new StringBuilder((String) legActionCompletedLegActionsQuestionAnswers.get("QuestionListVersion"));
																			StringBuilder legAction_AnswerListId = new StringBuilder((String) legActionCompletedLegActionsQuestionAnswers.get("AnswerListId"));
																			StringBuilder legAction_Signature = new StringBuilder(); legAction_Signature.append((String) legActionCompletedLegActionsQuestionAnswers.get("Signature"));
																			if(legAction_Signature.compareTo(new StringBuilder("null")) == 0) {legAction_Signature.setLength(0);}

																			Boolean legAction_HasPhoto = Boolean.valueOf((String) legActionCompletedLegActionsQuestionAnswers.get("QuestionListId"));
																			StringBuilder legAction_PhotoData = new StringBuilder(); legAction_PhotoData.append((String) legActionCompletedLegActionsQuestionAnswers.get("HasPhoto"));

																			StringBuilder legAction_PhotoDataUrl = new StringBuilder(); legAction_PhotoDataUrl.append((String) legActionCompletedLegActionsQuestionAnswers.get("PhotoDataUrl"));
																			if(legAction_PhotoDataUrl.compareTo(new StringBuilder("null")) == 0) {legAction_PhotoDataUrl.setLength(0);}

																			Boolean legAction_HasHighResImage = Boolean.valueOf((String) legActionCompletedLegActionsQuestionAnswers.get("HasHighResImage"));

																			StringBuilder legAction_HighResImageUrl = new StringBuilder(); legAction_HighResImageUrl.append((String) legActionCompletedLegActionsQuestionAnswers.get("HighResImageUrl"));
																			if(legAction_HighResImageUrl.compareTo(new StringBuilder("null")) == 0) {legAction_HighResImageUrl.setLength(0);}

																			try (
																					PreparedStatement psInsert = connection.prepareStatement(Queries.legActionsCompletedQAHeaderSql);
																			) {

																				psInsert.setInt(1, Utils.tryParseInt(fleet_id.toString(), 0));
																				psInsert.setInt(2, Utils.tryParseInt(vehicle_id.toString(), 0));
																				psInsert.setInt(3, Utils.tryParseInt(driver_id.toString(), 0));
																				psInsert.setTimestamp(4, new Timestamp(Utils.tryParseDate(finalGps_time.toString(), defaultDate).getTime()));
																				psInsert.setTimestamp(5, new Timestamp(Utils.tryParseDate(finalServer_time.toString(), defaultDate).getTime()));
																				psInsert.setFloat(6, Utils.tryParseFloat(finalLatitude.toString(), 0));
																				psInsert.setFloat(7, Utils.tryParseFloat(finalLongitude.toString(), 0));
																				psInsert.setInt(8, Utils.tryParseInt(finalDispatch_id.toString(), 0));
																				psInsert.setString(9, finalJob_reference.toString());
																				psInsert.setInt(10, Utils.tryParseInt(leg_LegVersion.toString(), 0));
																				psInsert.setInt(11, Utils.tryParseInt(leg_LegId.toString(), 0));
																				psInsert.setInt(12, Utils.tryParseInt(leg_LegType.toString(), 0));
																				psInsert.setInt(13, Utils.tryParseInt(leg_LegStatus.toString(), 0));
																				psInsert.setInt(14, Utils.tryParseInt(leg_LegNumber.toString(), 0));
																				psInsert.setString(15, leg_Title.toString());
																				psInsert.setInt(16, Utils.tryParseInt(leg_CustomerGroupId.toString(), 0));
																				psInsert.setInt(17, Utils.tryParseInt(leg_CustomerGroupVersion.toString(), 0));
																				psInsert.setInt(18, Utils.tryParseInt(leg_CustomerId.toString(), 0));
																				psInsert.setString(19, leg_LocationName.toString());
																				psInsert.setString(20, leg_Address.toString());
																				psInsert.setFloat(21, Utils.tryParseFloat(leg_Latitude.toString(), 0));
																				psInsert.setFloat(22, Utils.tryParseFloat(leg_Longitude.toString(), 0));
																				psInsert.setInt(23, Utils.tryParseInt(leg_Radius.toString(), 0));
																				psInsert.setInt(24, Utils.tryParseInt(legAction_QuestionListId.toString(), 0));
																				psInsert.setInt(25, Utils.tryParseInt(legAction_QuestionListVersion.toString(), 0));
																				psInsert.setString(26, legAction_Signature.toString());
																				psInsert.setBoolean(27, legAction_HasPhoto);
																				psInsert.setString(28, legAction_PhotoData.toString());
																				psInsert.setString(29, legAction_PhotoDataUrl.toString());
																				psInsert.setBoolean(30, legAction_HasHighResImage);
																				psInsert.setString(31, legAction_HighResImageUrl.toString());
																				psInsert.setBigDecimal(32, new BigDecimal(finalEventSequence.toString()));

																				psInsert.execute();

																			}
																			catch (SQLIntegrityConstraintViolationException sicve) {
																				// NOP
																			}
																			catch (SQLServerException sqle) {
																				log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																				log.error(sqle.getMessage());
																				log.error(sqle.getSQLState());
																			}
																			catch (Exception e) {
																				log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																				log.error("Error:", e);
																			}

																			// Process Questions & Answers
																			if (legAction_QuestionListId.toString().compareTo("9999") != 0) {
																				if (legActionCompletedLegActionsQuestionAnswers.containsKey("QuestionAnswer")) {

																					StringBuilder legActionsCompleted_QA_QuestionListId = new StringBuilder((String) legActionCompletedLegActionsQuestionAnswers.get("QuestionListId"));
																					StringBuilder legActionsCompleted_QA_QuestionListVersion = new StringBuilder((String) legActionCompletedLegActionsQuestionAnswers.get("QuestionListVersion"));

																					ArrayList legActionCompletedLegActionsQuestions = (ArrayList) legActionCompletedLegActionsQuestionAnswers.get("QuestionAnswer");

																					legActionCompletedLegActionsQuestions.forEach((legActionCompletedQuestionsObj) -> {

																						HashMap element = (HashMap) legActionCompletedQuestionsObj;

																						//ArrayList legActionCompletedLegActionsQuestionsConditional = (ArrayList) legActionCompletedLegActionsQuestionAnswers.get("ConditionalQuestionAnswers");

																						String legActionsCompleted_QA_QuestionId = (String) element.get("QuestionId");
																						String legActionsCompleted_QA_QuestionExternalRef = (String) element.get("QuestionExternalRef");
																						String legActionsCompleted_QA_AnswerId = (String) element.get("AnswerId");
																						String legActionsCompleted_QA_Answer = (String) element.get("Answer");
																						HashMap legActionsCompleted_QA_Conditional = (HashMap) element.get("ConditionalQuestionAnswers");

																						StringBuilder signature = new StringBuilder(); signature.append( (String) element.get("Signature"));
																						if(signature.compareTo(new StringBuilder("null")) == 0) {signature.setLength(0);}

																						Boolean hasPhotoData = Boolean.valueOf((String) element.get("HasPhoto"));
																						StringBuilder photoData = new StringBuilder(); photoData.append( (String) element.get("PhotoData"));

																						StringBuilder photoDataUrl = new StringBuilder(); photoDataUrl.append( (String) element.get("PhotoDataUrl"));
																						if(photoDataUrl.compareTo(new StringBuilder("null")) == 0) {photoDataUrl.setLength(0);}

																						Boolean hasHighResImage = Boolean.valueOf((String) element.get("HasHighResImage"));
																						StringBuilder highResImageUrl = new StringBuilder(); highResImageUrl.append( (String) element.get("HighResImageUrl"));
																						if(highResImageUrl.compareTo(new StringBuilder("null")) == 0) {highResImageUrl.setLength(0);}

																						if (legActionsCompleted_QA_Answer != null) {

																							try (
																									PreparedStatement psInsert = connection.prepareStatement(Queries.legActionsCompletedQASql);
																							) {

																								psInsert.setInt(1, Utils.tryParseInt(fleet_id.toString(), 0));
																								psInsert.setInt(2, Utils.tryParseInt(vehicle_id.toString(), 0));
																								psInsert.setInt(3, Utils.tryParseInt(driver_id.toString(), 0));
																								psInsert.setTimestamp(4, new Timestamp(Utils.tryParseDate(finalGps_time.toString(), defaultDate).getTime()));
																								psInsert.setTimestamp(5, new Timestamp(Utils.tryParseDate(finalServer_time.toString(), defaultDate).getTime()));
																								psInsert.setFloat(6, Utils.tryParseFloat(finalLatitude.toString(), 0));
																								psInsert.setFloat(7, Utils.tryParseFloat(finalLongitude.toString(), 0));
																								psInsert.setInt(8, Utils.tryParseInt(finalDispatch_id.toString(), 0));
																								psInsert.setString(9, finalJob_reference.toString());
																								psInsert.setInt(10, Utils.tryParseInt(leg_LegVersion.toString(), 0));
																								psInsert.setInt(11, Utils.tryParseInt(leg_LegId.toString(), 0));
																								psInsert.setInt(12, Utils.tryParseInt(leg_LegType.toString(), 0));
																								psInsert.setInt(13, Utils.tryParseInt(leg_LegStatus.toString(), 0));
																								psInsert.setInt(14, Utils.tryParseInt(leg_LegNumber.toString(), 0));
																								psInsert.setString(15, leg_Title.toString());
																								psInsert.setInt(16, Utils.tryParseInt(leg_CustomerGroupId.toString(), 0));
																								psInsert.setInt(17, Utils.tryParseInt(leg_CustomerGroupVersion.toString(), 0));
																								psInsert.setInt(18, Utils.tryParseInt(leg_CustomerId.toString(), 0));
																								psInsert.setString(19, leg_LocationName.toString());
																								psInsert.setString(20, leg_Address.toString());
																								psInsert.setFloat(21, Utils.tryParseFloat(leg_Latitude.toString(), 0));
																								psInsert.setFloat(22, Utils.tryParseFloat(leg_Longitude.toString(), 0));
																								psInsert.setInt(23, Utils.tryParseInt(leg_Radius.toString(), 0));
																								psInsert.setBigDecimal(24, new BigDecimal(finalEventSequence.toString()));
																								psInsert.setInt(25, Utils.tryParseInt(legActionsCompleted_QA_QuestionListId.toString(), 0));
																								psInsert.setInt(26, Utils.tryParseInt(legActionsCompleted_QA_QuestionListVersion.toString(), 0));
																								psInsert.setInt(27, Utils.tryParseInt(legActionsCompleted_QA_QuestionId, 0));
																								psInsert.setString(28, legActionsCompleted_QA_QuestionExternalRef);
																								psInsert.setInt(29, Utils.tryParseInt(legActionsCompleted_QA_AnswerId, 0));
																								psInsert.setString(30, legActionsCompleted_QA_Answer);
																								psInsert.setString(31, signature.toString());
																								psInsert.setBoolean(32, hasPhotoData);
																								psInsert.setString(33, photoData.toString());
																								psInsert.setString(34, photoDataUrl.toString());
																								psInsert.setBoolean(35, hasHighResImage);
																								psInsert.setString(36, highResImageUrl.toString());

																								psInsert.execute();


																							}
																							// Handle any errors that may have occurred.
																							catch (SQLIntegrityConstraintViolationException sicve) {
																								// NOP
																							}
																							catch (SQLServerException sqle) {
																								log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																								log.error(sqle.getMessage());
																								log.error(sqle.getSQLState());
																							}
																							catch (Exception e) {
																								log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																								log.error("Error:", e);
																							}
																						}

																						if (legActionsCompleted_QA_Conditional != null) {
																							StringBuilder legActionsCompleted_QA_Conditional_QuestionListId = new StringBuilder(); legActionsCompleted_QA_Conditional_QuestionListId.append((String) legActionsCompleted_QA_Conditional.get("QuestionListId"));
																							StringBuilder legActionsCompleted_QA_Conditional_QuestionListVersion = new StringBuilder(); legActionsCompleted_QA_Conditional_QuestionListVersion.append((String) legActionsCompleted_QA_Conditional.get("QuestionListVersion"));
																							StringBuilder  legActionsCompleted_QA_Conditional_AnswerListId = new StringBuilder(); legActionsCompleted_QA_Conditional_AnswerListId.append((String) legActionsCompleted_QA_Conditional.get("AnswerListId"));
																							StringBuilder  legActionsCompleted_QA_Conditional_Signature = new StringBuilder(); legActionsCompleted_QA_Conditional_Signature.append((String) legActionsCompleted_QA_Conditional.get("Signature"));
																							ArrayList legActionsCompleted_QA_Conditional_QA = (ArrayList) legActionsCompleted_QA_Conditional.get("QuestionAnswer");

																							legActionsCompleted_QA_Conditional_QA.forEach((legActionCompletedQuestionsConditionalObj) -> {

																								HashMap elementLegActionsCompleteQaConditional = (HashMap) legActionCompletedQuestionsConditionalObj;
																								StringBuilder lacqac_questionId = new StringBuilder(); lacqac_questionId.append((String) elementLegActionsCompleteQaConditional.get("QuestionId"));
																								StringBuilder lacqac_questionExternalRef = new StringBuilder(); lacqac_questionExternalRef.append((String) elementLegActionsCompleteQaConditional.get("QuestionExternalRef"));
																								StringBuilder lacqac_answerId = new StringBuilder(); lacqac_answerId.append((String) elementLegActionsCompleteQaConditional.get("AnswerId"));
																								StringBuilder lacqac_answer = new StringBuilder(); lacqac_answer.append((String) elementLegActionsCompleteQaConditional.get("Answer"));

																								StringBuilder lacqac_signature = new StringBuilder(); lacqac_signature.append((String) elementLegActionsCompleteQaConditional.get("Signature"));
																								if(lacqac_signature.compareTo(new StringBuilder("null")) == 0) {lacqac_signature.setLength(0);}

																								Boolean lacqac_hasPhoto = Boolean.valueOf((String) elementLegActionsCompleteQaConditional.get("HasPhoto"));
																								StringBuilder lacqac_photoData = new StringBuilder(); lacqac_photoData.append((String) elementLegActionsCompleteQaConditional.get("PhotoData"));

																								StringBuilder lacqac_photoDataUrl = new StringBuilder(); lacqac_photoDataUrl.append((String) elementLegActionsCompleteQaConditional.get("PhotoDataUrl"));
																								if(lacqac_photoDataUrl.compareTo(new StringBuilder("null")) == 0) {lacqac_photoDataUrl.setLength(0);}

																								Boolean lacqac_hasHighResImage = Boolean.valueOf((String) elementLegActionsCompleteQaConditional.get("HasHighResImage"));

																								StringBuilder lacqac_highResImageUrl= new StringBuilder(); lacqac_highResImageUrl.append((String) elementLegActionsCompleteQaConditional.get("HighResImageUrl"));
																								if(lacqac_highResImageUrl.compareTo(new StringBuilder("null")) == 0) {lacqac_highResImageUrl.setLength(0);}

																								String parentQuestionId = legActionsCompleted_QA_QuestionId;

																								try (
																										PreparedStatement psInsert = connection.prepareStatement(Queries.legActionsCompletedQAConditionalSql);
																								) {

																									psInsert.setInt(1, Utils.tryParseInt(legActionsCompleted_QA_QuestionId.toString(), 0));
																									psInsert.setInt(2, Utils.tryParseInt(legActionsCompleted_QA_AnswerId.toString(), 0));
																									psInsert.setInt(3, Utils.tryParseInt(lacqac_questionId.toString(), 0));
																									psInsert.setString(4, lacqac_questionExternalRef.toString());
																									psInsert.setInt(5, Utils.tryParseInt(lacqac_answerId.toString(), 0));
																									psInsert.setString(6, lacqac_answer.toString());
																									psInsert.setString(7, lacqac_signature.toString());

																									psInsert.setBoolean(8, lacqac_hasPhoto);
																									psInsert.setString(9, lacqac_photoData.toString());
																									psInsert.setString(10, lacqac_photoDataUrl.toString());
																									psInsert.setBoolean(11, lacqac_hasHighResImage);
																									psInsert.setString(12, lacqac_highResImageUrl.toString());
																									psInsert.setBigDecimal(13, new BigDecimal(finalEventSequence.toString()));

																									psInsert.execute();


																								}
																								// Handle any errors that may have occurred.
																								catch (SQLIntegrityConstraintViolationException sicve) {
																									// NOP
																								}
																								catch (SQLServerException sqle) {
																									log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																									log.error(sqle.getMessage());
																									log.error(sqle.getSQLState());
																								}
																								catch (Exception e) {
																									log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																									log.error("Error:", e);
																								}

																							});

																						}

																					});
																				}
																			}
																		}
																	}
																}


																// here needed


																// Leg Action 16 / 17 - Pickup / Dropoff
																if (Integer.parseInt(legActionType.toString()) == 16 || Integer.parseInt(legActionType.toString()) == 17) {

																	if (legActionDataElement.containsKey("CompletedLegActions")) {
																		HashMap legActionCompletedLegActions = (HashMap) legActionDataElement.get("CompletedLegActions");

																		StringBuilder legActionsCompleted_Leg_Action_Id = new StringBuilder((String) legActionCompletedLegActions.get("CompletedLegActionId"));

																		if (legActionCompletedLegActions.get("Items").getClass().getTypeName().compareTo("java.util.ArrayList") == 0) {

																			ArrayList legActionCompletedLegActionsItems = (ArrayList) legActionCompletedLegActions.get("Items");

																			legActionCompletedLegActionsItems.forEach((legActionCompletedItemsObj) -> {

																				HashMap legActionCompletedItemsElement = (HashMap) legActionCompletedItemsObj;

																				StringBuilder itemId = new StringBuilder(); itemId.append((String) legActionCompletedItemsElement.get("ItemId"));
																				StringBuilder itemListId = new StringBuilder(); itemListId.append((String) legActionCompletedItemsElement.get("ItemListId"));
																				StringBuilder itemListVersion = new StringBuilder(); itemListVersion.append((String) legActionCompletedItemsElement.get("ItemListVersion"));
																				StringBuilder itemExternalReference = new StringBuilder(); itemExternalReference.append((String) legActionCompletedItemsElement.get("ExternalReference"));
																				StringBuilder itemCount = new StringBuilder(); itemCount.append((String) legActionCompletedItemsElement.get("Count"));
																				StringBuilder itemHasAdjustment = new StringBuilder(); itemHasAdjustment.append((String) legActionCompletedItemsElement.get("HasAdjustment"));
																				StringBuilder itemSeqId = new StringBuilder(); itemSeqId.append((String) legActionCompletedItemsElement.get("Id"));
																				StringBuilder itemName = new StringBuilder(); itemName.append((String) legActionCompletedItemsElement.get("Name"));

																				if (legActionCompletedItemsElement != null) {

																					try (
																							PreparedStatement psInsert = connection.prepareStatement(Queries.legActionsCompletedItems);
																					) {

																						psInsert.setInt(1, Utils.tryParseInt(leg_LegVersion.toString(), 0));
																						psInsert.setInt(2, Utils.tryParseInt(leg_LegId.toString(), 0));
																						psInsert.setInt(3, Utils.tryParseInt(leg_LegType.toString(), 0));
																						psInsert.setInt(4, Utils.tryParseInt(leg_LegStatus.toString(), 0));
																						psInsert.setInt(5, Utils.tryParseInt(leg_LegNumber.toString(), 0));

																						psInsert.setInt(6, Utils.tryParseInt(legActionType.toString(), 0));
																						psInsert.setInt(7, Utils.tryParseInt(legActionId.toString(), 0));
																						psInsert.setInt(8, Utils.tryParseInt(legActionStatus.toString(), 0));
																						psInsert.setInt(9, Utils.tryParseInt(legActionNumber.toString(), 0));

																						psInsert.setInt(10, Utils.tryParseInt(itemId.toString(), 0));
																						psInsert.setInt(11, Utils.tryParseInt(itemListId.toString(), 0));
																						psInsert.setInt(12, Utils.tryParseInt(itemSeqId.toString(), 0));
																						psInsert.setInt(13, Utils.tryParseInt(itemListVersion.toString(), 0));
																						psInsert.setString(14, itemExternalReference.toString());
																						psInsert.setString(15, itemName.toString());
																						psInsert.setInt(16, Utils.tryParseInt(itemCount.toString(), 0));
																						psInsert.setBoolean(17, Boolean.parseBoolean(itemHasAdjustment.toString()));


																						psInsert.setBigDecimal(18, new BigDecimal(finalEventSequence));

																						psInsert.execute();


																					}
																					// Handle any errors that may have occurred.
																					catch (SQLIntegrityConstraintViolationException sicve) {
																						// NOP
																					}
																					catch (SQLServerException sqle) {
																						log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																						log.error(sqle.getMessage());
																						log.error(sqle.getSQLState());
																					}
																					catch (Exception e) {
																						log.error("Error Processing Event Sequence - " + finalEventSequence.toString());
																						log.error("Error:", e);
																					}
																				}

																			});
																		}
																	}

																	processEventForDigitalTwin(new BigDecimal(finalEventSequence), Utils.tryParseInt(leg_LegNumber.toString(), 0), Utils.tryParseInt(leg_LegId.toString(), 0));
																}

															});

														}


													}

												});

											}


										}


									}


								}

							}

							// Update Watermark
							try (
									PreparedStatement prepsUpdateWatermark = connection.prepareStatement(Queries.updateWatermarkSql);
							) {
								prepsUpdateWatermark.setBigDecimal(1, new BigDecimal(eventSequence));
								prepsUpdateWatermark.execute();
							}
							// Handle any errors that may have occurred.
							catch (SQLIntegrityConstraintViolationException sicve) {
								// NOP
							}
							catch (SQLServerException sqle) {
								log.error("Error Processing Event Sequence - " + eventSequence.toString());
								log.error(sqle.getMessage());
								log.error(sqle.getSQLState());
							}
							catch (Exception e) {
								log.error("Error Processing Event Sequence - " + eventSequence.toString());
								log.error("Error:", e);
							}
						}
					}
				}
				catch (Exception e) {
					// TODO: Add specific error logging for failed records to be consumed by a future monitoring system
					// If this exception has been reached then this Kinesis event is unprocessed and needs to be followed up on
					log.error("Error:", e);
				}

				Thread.sleep(pollingInterval);

			}
		}
		catch(Exception e)
		{
			log.error("Error:", e);
		}
	}

	public static String nullToEmpty(String text) {
		return text == null ? "" : text;
	}

	class JobEventDataComparator implements Comparator<HashMap>{

		public int compare(HashMap o1, HashMap o2) {

			return new CompareToBuilder()
					.append(o1.get("GpsTime").toString(), o2.get("GpsTime").toString())
					.append(o1.get("LegId").toString(), o2.get("LegId").toString()).toComparison();

		}

	}

	private static HashMap<String, Object> createHashMapFromJsonString(String json) {

		try {
			JsonObject object = (JsonObject) JsonParser.parseString(json);
			Set<Map.Entry<String, JsonElement>> set = object.entrySet();
			Iterator<Map.Entry<String, JsonElement>> iterator = set.iterator();
			HashMap<String, Object> map = new HashMap<String, Object>();

			while (iterator.hasNext()) {

				Map.Entry<String, JsonElement> entry = iterator.next();
				String key = entry.getKey();
				JsonElement value = entry.getValue();

				if (null != value) {
					if (!value.isJsonPrimitive()) {
						if (value.isJsonObject()) {

							map.put(key, createHashMapFromJsonString(value.toString()));
						} else if (value.isJsonArray() && value.toString().contains(":")) {

							List<HashMap<String, Object>> list = new ArrayList<>();
							JsonArray array = value.getAsJsonArray();
							if (null != array) {
								for (JsonElement element : array) {
									list.add(createHashMapFromJsonString(element.toString()));
								}
								map.put(key, list);
							}
						} else if (value.isJsonArray() && !value.toString().contains(":")) {
							map.put(key, value.getAsJsonArray());
						}
					} else {
						map.put(key, value.getAsString());
					}
				}
			}
			return map;
		}
		catch (Exception e)
		{
			log.error("Error:", e);
			log.debug(json);
		}
		return null;
	}

	private boolean processEventForDigitalTwin(BigDecimal eventSequence, int legNumber, int legId) {

		log.debug("Call SP processEventForDigitalTwin with : legNumber=" + legNumber + ", legId=" + legId);

		try (Connection connection = dataSource.getConnection())
		{

			CallableStatement cstmt = null;
			ResultSet rs = null;

			cstmt = connection.prepareCall(
					"{call [MTDATA_KINESIS].[dbo].[processEventForDigitalTwin](?, ?, ?)}",
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);

			cstmt.setBigDecimal(1, eventSequence);
			cstmt.setInt(2, legNumber);
			cstmt.setInt(3, legId);
			boolean results = cstmt.execute();
			int rowsAffected = 0;

			// Protects against lack of SET NOCOUNT in stored procedure
			while (results || rowsAffected != -1) {
				if (results) {
					rs = cstmt.getResultSet();
					break;
				} else {
					rowsAffected = cstmt.getUpdateCount();
				}
				results = cstmt.getMoreResults();
			}

		} catch (com.microsoft.sqlserver.jdbc.SQLServerException sqle) {
			log.error("Couldn't execute completedLegProcess ".concat(eventSequence.toString().concat("    ".concat(Integer.toString(legId)))) );
			log.error(sqle.getMessage());
			log.error(sqle.getSQLState());
		} catch (Exception e) {
			log.error("Error:", e);
			return false;
		}
		return true;
	}

	private boolean processDeliveryLegComplete(BigDecimal eventSequence, int legNumber, int legId) {

		try (Connection connection = dataSource.getConnection())
		{

			CallableStatement cstmt = null;
			ResultSet rs = null;

			cstmt = connection.prepareCall(
					"{call [MTDATA_KINESIS].[dbo].[completedLegProcess](?, ?, ?)}",
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);

			cstmt.setBigDecimal(1, eventSequence);
			cstmt.setInt(2, legNumber);
			cstmt.setInt(3, legId);
			boolean results = cstmt.execute();
			int rowsAffected = 0;

			// Protects against lack of SET NOCOUNT in stored procedure
			while (results || rowsAffected != -1) {
				if (results) {
					rs = cstmt.getResultSet();
					break;
				} else {
					rowsAffected = cstmt.getUpdateCount();
				}
				results = cstmt.getMoreResults();
			}

		} catch (com.microsoft.sqlserver.jdbc.SQLServerException sqle) {
			// Key Constraint errors ignored
			// These are caused by records being reread from a shard that have already
			// been committed to the database
			//if(sqle.getSQLState().compareTo("23000") != 0)
			//{
			log.error("Couldn't execute completedLegProcess ".concat(eventSequence.toString().concat("    ".concat(Integer.toString(legId)))) );
			log.error(sqle.getMessage());
			log.error(sqle.getSQLState());
			//}

		} catch (Exception e) {
			log.error("Error:", e);
			return false;
		}
		return true;

	}

	private String GetTestJSON() throws IOException {

		String content = null;


		//File file = new File(getClass().getResource("leg_complete_test.json").getFile());
		File file = new File("D:\\data\\clients\\glen cameron group\\dev\\kinesis_logistics_event_processor\\src\\main\\resources\\leg_complete_test.json");
		//File file = new File("D:\\data\\clients\\glen cameron group\\dev\\kinesis_logistics_event_processor\\src\\main\\resources\\job_complete_tas_empty.json");
		content = new String(Files.readAllBytes(file.toPath()));

		return content;

	}
}
