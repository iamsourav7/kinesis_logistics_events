package au.com.camerons.mtdata.kinesis.logistics;

public class Queries {

    static String eventBreakFinishedEarlySql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].events_break_finished_early" +
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
            "      ,?" +            // sequence_id
            ") ";

    static String eventBreakMissedSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].events_break_missed" +
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
            "      ,?" +            // sequence_id
            ") ";

    static String eventBreakOffSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].events_break_off" +
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
                    "      ,?" +            // sequence_id
                    ") ";

    static String eventBreakOnSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].events_break_on" +
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
                    "      ,?" +            // sequence_id
                    ") ";

    static String eventSiteArriveSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].events_site_arrive" +
                    "(" +
                    "     report_id" +
                    "    ,fleet_id" +
                    "    ,fleet_name" +
                    "    ,vehicle_id" +
                    "    ,vehicle_name" +
                    "    ,vehicle_external_reference" +
                    "    ,driver_id" +
                    "    ,driver_name" +
                    "    ,driver_external_reference" +
                    "    ,dispatch_id" +
                    "    ,leg_id" +
                    "    ,event_id" +
                    "    ,job_status" +
                    "    ,gps_time" +
                    "    ,server_time" +
                    "    ,latitude" +
                    "    ,longitude" +
                    "    ,odometer" +
                    "    ,total_fuel" +
                    "    ,address_number" +
                    "    ,address_street_name" +
                    "    ,address_suburb" +
                    "    ,address_city" +
                    "    ,address_state" +
                    "    ,address_postal_code" +
                    "    ,address_country" +
                    "    ,address_timezone_offset" +
                    "    ,sequence_id" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +            // report_id
                    "      ,?" +            // fleet_id
                    "      ,?" +            // fleet_name
                    "      ,?" +            // vehicle_id
                    "      ,?" +            // vehicle_name
                    "      ,?" +            // vehicle_external_reference
                    "      ,?" +            // driver_id
                    "      ,?" +            // driver_name
                    "      ,?" +            // driver_external_reference
                    "      ,?" +            // dispatch_id
                    "      ,?" +            // leg_id
                    "      ,?" +            // event_id
                    "      ,?" +            // job_status
                    "      ,?" +            // gps_time
                    "      ,?" +            // server_time
                    "      ,?" +            // latitude
                    "      ,?" +            // longitude
                    "      ,?" +            // odometer
                    "      ,?" +            // total_fuel
                    "      ,?" +            // address_number
                    "      ,?" +            // address_street_name
                    "      ,?" +            // address_suburb
                    "      ,?" +            // address_city
                    "      ,?" +            // address_state
                    "      ,?" +            // address_postal_code
                    "      ,?" +            // address_country
                    "      ,?" +            // address_timezone_offset
                    "      ,?" +            // sequence_id"
                    ") ";

    static String eventSiteDepartSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].events_site_depart" +
                    "(" +
                    "     report_id" +
                    "    ,fleet_id" +
                    "    ,fleet_name" +
                    "    ,vehicle_id" +
                    "    ,vehicle_name" +
                    "    ,vehicle_external_reference" +
                    "    ,driver_id" +
                    "    ,driver_name" +
                    "    ,driver_external_reference" +
                    "    ,dispatch_id" +
                    "    ,leg_id" +
                    "    ,event_id" +
                    "    ,job_status" +
                    "    ,gps_time" +
                    "    ,server_time" +
                    "    ,latitude" +
                    "    ,longitude" +
                    "    ,odometer" +
                    "    ,total_fuel" +
                    "    ,address_number" +
                    "    ,address_street_name" +
                    "    ,address_suburb" +
                    "    ,address_city" +
                    "    ,address_state" +
                    "    ,address_postal_code" +
                    "    ,address_country" +
                    "    ,address_timezone_offset" +
                    "    ,sequence_id" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +            // report_id
                    "      ,?" +            // fleet_id
                    "      ,?" +            // fleet_name
                    "      ,?" +            // vehicle_id
                    "      ,?" +            // vehicle_name
                    "      ,?" +            // vehicle_external_reference
                    "      ,?" +            // driver_id
                    "      ,?" +            // driver_name
                    "      ,?" +            // driver_external_reference
                    "      ,?" +            // dispatch_id
                    "      ,?" +            // leg_id
                    "      ,?" +            // event_id
                    "      ,?" +            // job_status
                    "      ,?" +            // gps_time
                    "      ,?" +            // server_time
                    "      ,?" +            // latitude
                    "      ,?" +            // longitude
                    "      ,?" +            // odometer
                    "      ,?" +            // total_fuel
                    "      ,?" +            // address_number
                    "      ,?" +            // address_street_name
                    "      ,?" +            // address_suburb
                    "      ,?" +            // address_city
                    "      ,?" +            // address_state
                    "      ,?" +            // address_postal_code
                    "      ,?" +            // address_country
                    "      ,?" +            // address_timezone_offset
                    "      ,?" +            // sequence_id"
                    ") ";

    static String legStatusSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].leg_status" +
                    "(" +
                    "     report_id" +
                    "    ,fleet_id" +
                    "    ,vehicle_id" +
                    "    ,vehicle_name" +
                    "    ,driver_id" +
                    "    ,driver_name" +
                    "    ,driver_external_reference" +
                    "    ,gps_time" +
                    "    ,server_time" +
                    "    ,latitude" +
                    "    ,longitude" +
                    "    ,dispatch_id" +
                    "    ,job_reference" +
                    "    ,job_reference_type" +
                    "    ,job_reference_int" +
                    "    ,job_type" +
                    "    ,job_status" +
                    "    ,docket_number" +
                    "    ,leg_version" +
                    "    ,leg_id" +
                    "    ,leg_type" +
                    "    ,leg_status" +
                    "    ,leg_number" +
                    "    ,leg_title" +
                    "    ,leg_customer_group_id" +
                    "    ,leg_customer_group_version" +
                    "    ,leg_customer_id" +
                    "    ,leg_location_name" +
                    "    ,leg_address" +
                    "    ,leg_latitude" +
                    "    ,leg_longitude" +
                    "    ,leg_radius" +
                    "    ,sequence_id" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +            // report_id
                    "      ,?" +            // fleet_id
                    "      ,?" +            // vehicle_id
                    "      ,?" +            // vehicle_name
                    "      ,?" +            // driver_id
                    "      ,?" +            // driver_name
                    "      ,?" +            // driver_external_reference
                    "      ,?" +            // gps_time
                    "      ,?" +            // server_time
                    "      ,?" +            // latitude
                    "      ,?" +            // longitude
                    "      ,?" +            // dispatch_id
                    "      ,?" +            // job_reference
                    "      ,?" +            // job_reference_type
                    "      ,?" +            // job_reference_int
                    "      ,?" +            // job_type
                    "      ,?" +            // job_status
                    "      ,?" +            // docket_number
                    "      ,?" +            // leg_version
                    "      ,?" +            // leg_id
                    "      ,?" +            // leg_type
                    "      ,?" +            // leg_status
                    "      ,?" +            // leg_number
                    "      ,?" +            // leg_title
                    "      ,?" +            // leg_customer_group_id
                    "      ,?" +            // leg_customer_group_version
                    "      ,?" +            // leg_customer_id
                    "      ,?" +            // leg_location_name
                    "      ,?" +            // leg_address
                    "      ,?" +            // leg_latitude
                    "      ,?" +            // leg_longitude
                    "      ,?" +            // leg_radius
                    "      ,?" +            // sequence_id
                    ") ";

    static String legStartActionsSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].leg_start_actions" +
            "(" +
            "     report_id" +
            "    ,fleet_id" +
            "    ,vehicle_id" +
            "    ,vehicle_name" +
            "    ,driver_id" +
            "    ,driver_name" +
            "    ,driver_external_reference" +
            "    ,gps_time" +
            "    ,server_time" +
            "    ,latitude" +
            "    ,longitude" +
            "    ,dispatch_id" +
            "    ,job_status" +
            "    ,leg_id" +
            "    ,leg_number" +
            "    ,leg_type" +
            "    ,leg_status" +
            "    ,leg_title" +
            "    ,leg_action_type" +
            "    ,leg_action_id" +
            "    ,leg_action_status" +
            "    ,leg_action_number" +
            "    ,sequence_id" +
            ")" +
            "VALUES" +
            "(" +
            "       ?" +            // report_id
            "      ,?" +            // fleet_id
            "      ,?" +            // vehicle_id
            "      ,?" +            // vehicle_name
            "      ,?" +            // driver_id
            "      ,?" +            // driver_name
            "      ,?" +            // driver_external_reference
            "      ,?" +            // gps_time
            "      ,?" +            // server_time
            "      ,?" +            // latitude
            "      ,?" +            // longitude
            "      ,?" +            // dispatch_id
            "      ,?" +            // job_status
            "      ,?" +            // leg_id
            "      ,?" +            // leg_number
            "      ,?" +            // leg_type
            "      ,?" +            // leg_status
            "      ,?" +            // leg_title
            "      ,?" +            // leg_action_type
            "      ,?" +            // leg_action_id
            "      ,?" +            // leg_action_status
            "      ,?" +            // leg_action_number
            "      ,?" +            // sequence_id
            ") ";

    static String legCompleteInsertSql =
            "INSERT INTO [MTDATA_KINESIS].dbo.leg_complete" +
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
                    "    ,dispatch_id" +
                    "    ,job_reference" +
                    "    ,job_reference_type" +
                    "    ,job_reference_int" +
                    "    ,job_type" +
                    "    ,job_status" +
                    "    ,job_title" +
                    "    ,docket_number" +
                    "    ,address_number" +
                    "    ,address_street_name" +
                    "    ,address_suburb" +
                    "    ,address_city" +
                    "    ,address_state" +
                    "    ,address_postal_code" +
                    "    ,address_country" +
                    "    ,address_timezone_offset" +
                    "    ,leg_version" +
                    "    ,leg_id" +
                    "    ,leg_type" +
                    "    ,leg_status" +
                    "    ,leg_number" +
                    "    ,leg_external_reference" +
                    "    ,leg_title" +
                    "    ,leg_customer_group_id" +
                    "    ,leg_customer_group_version" +
                    "    ,leg_customer_id" +
                    "    ,leg_customer_external_reference" +
                    "    ,leg_location_name" +
                    "    ,leg_address" +
                    "    ,leg_latitude" +
                    "    ,leg_longitude" +
                    "    ,leg_radius" +
                    "    ,leg_arrive_time" +
                    "    ,leg_arrive_time_tolerance" +
                    "    ,leg_depart_time" +
                    "    ,leg_depart_time_tolerance" +
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
                    "      ,?" +            // dispatch_id
                    "      ,?" +            // job_reference
                    "      ,?" +            // job_reference_type
                    "      ,?" +            // job_reference_int
                    "      ,?" +            // job_type
                    "      ,?" +            // job_status
                    "      ,?" +            // job_title
                    "      ,?" +            // docket_number
                    "      ,?" +            // address_number
                    "      ,?" +            // address_street_name
                    "      ,?" +            // address_suburb
                    "      ,?" +            // address_city
                    "      ,?" +            // address_state
                    "      ,?" +            // address_postal_code
                    "      ,?" +            // address_country
                    "      ,?" +            // address_timezone_offset
                    "      ,?" +            // leg_version
                    "      ,?" +            // leg_id
                    "      ,?" +            // leg_type
                    "      ,?" +            // leg_status
                    "      ,?" +            // leg_number
                    "      ,?" +            // leg_external_reference
                    "      ,?" +            // leg_title
                    "      ,?" +            // leg_customer_group_id
                    "      ,?" +            // leg_customer_group_version
                    "      ,?" +            // leg_customer_id
                    "      ,?" +            // leg_customer_external_reference
                    "      ,?" +            // leg_location_name
                    "      ,?" +            // leg_address
                    "      ,?" +            // leg_latitude
                    "      ,?" +            // leg_longitude
                    "      ,?" +            // leg_radius
                    "      ,?" +            // leg_arrive_time
                    "      ,?" +            // leg_arrive_time_tolerance
                    "      ,?" +            // leg_depart_time
                    "      ,?" +            // leg_depart_time_tolerance
                    "      ,?" +            // sequence_id
                    ") ";

    static String legCompleteItemsSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].leg_complete_items" +
                    "(" +
                    " leg_version" +
                    ",leg_id" +
                    ",leg_type" +
                    ",leg_status" +
                    ",leg_number" +
                    ",leg_action_type" +
                    ",leg_action_id" +
                    ",leg_action_status" +
                    ",leg_action_number" +
                    ",item_id" +
                    ",item_list_id" +
                    ",item_seq_id" +
                    ",item_list_version" +
                    ",item_external_reference" +
                    ",item_name" +
                    ",item_count" +
                    ",sequence_id" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    ") ";

    static String legActionsCompletedQAHeaderSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].leg_complete_qa_header" +
                    "(" +
                    "     fleet_id" +
                    "    ,vehicle_id" +
                    "    ,driver_id" +
                    "    ,gps_time" +
                    "    ,server_time" +
                    "    ,latitude" +
                    "    ,longitude" +
                    "    ,dispatch_id" +
                    "    ,job_reference" +
                    "    ,leg_version" +
                    "    ,leg_id" +
                    "    ,leg_type" +
                    "    ,leg_status" +
                    "    ,leg_number" +
                    "    ,leg_title" +
                    "    ,leg_customer_group_id" +
                    "    ,leg_customer_group_version" +
                    "    ,leg_customer_id" +
                    "    ,leg_location_name" +
                    "    ,leg_address" +
                    "    ,leg_latitude" +
                    "    ,leg_longitude" +
                    "    ,leg_radius" +
                    "    ,question_list_id" +
                    "    ,question_list_version" +
                    "	 ,signature" +
                    "	 ,has_photo_data" +
                    "	 ,photo_data" +
                    "	 ,photo_data_url" +
                    "	 ,has_high_res_image" +
                    "	 ,high_res_image_url" +
                    "    ,sequence_id" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    ") ";

    static String legActionsCompletedQASql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].leg_complete_qa" +
                    "(" +
                    "     fleet_id" +
                    "    ,vehicle_id" +
                    "    ,driver_id" +
                    "    ,gps_time" +
                    "    ,server_time" +
                    "    ,latitude" +
                    "    ,longitude" +
                    "    ,dispatch_id" +
                    "    ,job_reference" +
                    "    ,leg_version" +
                    "    ,leg_id" +
                    "    ,leg_type" +
                    "    ,leg_status" +
                    "    ,leg_number" +
                    "    ,leg_title" +
                    "    ,leg_customer_group_id" +
                    "    ,leg_customer_group_version" +
                    "    ,leg_customer_id" +
                    "    ,leg_location_name" +
                    "    ,leg_address" +
                    "    ,leg_latitude" +
                    "    ,leg_longitude" +
                    "    ,leg_radius" +
                    "    ,sequence_id" +
                    "    ,question_list_id" +
                    "    ,question_list_version" +
                    "    ,question_id" +
                    "    ,question_external_ref" +
                    "    ,answer_id" +
                    "    ,answer" +
                    "	 ,signature" +
                    "	 ,has_photo_data" +
                    "	 ,photo_data" +
                    "	 ,photo_data_url" +
                    "	 ,has_high_res_image" +
                    "	 ,high_res_image_url" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    ") ";

   static String legActionsCompletedQAConditionalSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].leg_complete_qa_conditional" +
                    "(" +
                    "     parent_question_id" +
                    "    ,parent_answer_id" +
                    "    ,question_id" +
                    "    ,question_external_ref" +
                    "    ,answer_id" +
                    "    ,answer" +
                    "    ,signature" +
                    "    ,has_photo_data" +
                    "    ,photo_data" +
                    "    ,photo_data_url" +
                    "    ,has_high_res_image" +
                    "    ,high_res_image_url" +
                    "    ,sequence_id" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    ") ";

    static String legActionsCompletedItems =
            "INSERT INTO [MTDATA_KINESIS].[dbo].leg_complete_action_items" +
                    "(" +
                    " leg_version" +
                    ",leg_id" +
                    ",leg_type" +
                    ",leg_status" +
                    ",leg_number" +
                    ",leg_action_type" +
                    ",leg_action_id" +
                    ",leg_action_status" +
                    ",leg_action_number" +
                    ",item_id" +
                    ",item_list_id" +
                    ",item_seq_id" +
                    ",item_list_version" +
                    ",item_external_reference" +
                    ",item_name" +
                    ",item_count" +
                    ",has_adjustment" +
                    ",sequence_id" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    "      ,?" +
                    ") ";



    static String eventsWaypointArriveSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].events_waypoint_arrive" +
                    "(" +
                    "     report_id" +
                    "    ,fleet_id" +
                    "    ,fleet_name" +
                    "    ,vehicle_id" +
                    "    ,vehicle_name" +
                    "    ,vehicle_external_reference" +
                    "    ,driver_id" +
                    "    ,driver_name" +
                    "    ,driver_external_reference" +
                    "    ,gps_time" +
                    "    ,device_time" +
                    "    ,latitude" +
                    "    ,longitude" +
                    "    ,speed_accumulator" +
                    "    ,direction" +
                    "    ,speed" +
                    "    ,maximum_speed" +
                    "    ,place_name" +
                    "    ,map_reference" +
                    "    ,hdop" +
                    "    ,num_satellites_in_view" +
                    "    ,altitude" +
                    "    ,odometer" +
                    "    ,total_fuel" +
                    "    ,device_serial_number" +
                    "    ,device_id" +
                    "    ,address_number" +
                    "    ,address_street_name" +
                    "    ,address_suburb" +
                    "    ,address_city" +
                    "    ,address_state" +
                    "    ,address_postal_code" +
                    "    ,address_country" +
                    "    ,address_timezone_offset" +
                    "    ,waypoint_set_point_id" +
                    "    ,waypoint_set_point_name" +
                    "    ,waypoint_external_ref" +
                    "    ,sequence_id" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +            // report_id
                    "      ,?" +            // fleet_id
                    "      ,?" +            // fleet_name
                    "      ,?" +            // vehicle_id
                    "      ,?" +            // vehicle_name
                    "      ,?" +            // vehicle_external_reference
                    "      ,?" +            // driver_id
                    "      ,?" +            // driver_name
                    "      ,?" +            // driver_external_reference
                    "      ,?" +            // gps_time
                    "      ,?" +            // device_time
                    "      ,?" +            // latitude
                    "      ,?" +            // longitude
                    "      ,?" +            // speed accumulation
                    "      ,?" +            // direction
                    "      ,?" +            // speed
                    "      ,?" +            // maximum_speed
                    "      ,?" +            // place_name
                    "      ,?" +            // map_reference
                    "      ,?" +            // hdop
                    "      ,?" +            // num_sattelites_in_view
                    "      ,?" +            // altitude
                    "      ,?" +            // odometer
                    "      ,?" +            // total_fuel
                    "      ,?" +            // device_serial_number
                    "      ,?" +            // device_id
                    "      ,?" +            // address_number
                    "      ,?" +            // address_street_name
                    "      ,?" +            // address_suburb
                    "      ,?" +            // address_city
                    "      ,?" +            // address_state
                    "      ,?" +            // address_postal_code
                    "      ,?" +            // address_country
                    "      ,?" +            // address_timezone_offset
                    "      ,?" +            // waypoint_set_point_id
                    "      ,?" +            // waypoint_set_point_name
                    "      ,?" +            // waypoint_external_ref
                    "      ,?" +            // sequence_id"
                    ") ";

    static String eventsWaypointDepartSql =
            "INSERT INTO [MTDATA_KINESIS].[dbo].events_waypoint_depart" +
                    "(" +
                    "     report_id" +
                    "    ,fleet_id" +
                    "    ,fleet_name" +
                    "    ,vehicle_id" +
                    "    ,vehicle_name" +
                    "    ,vehicle_external_reference" +
                    "    ,driver_id" +
                    "    ,driver_name" +
                    "    ,driver_external_reference" +
                    "    ,gps_time" +
                    "    ,device_time" +
                    "    ,latitude" +
                    "    ,longitude" +
                    "    ,speed_accumulator" +
                    "    ,direction" +
                    "    ,speed" +
                    "    ,maximum_speed" +
                    "    ,place_name" +
                    "    ,map_reference" +
                    "    ,hdop" +
                    "    ,num_satellites_in_view" +
                    "    ,altitude" +
                    "    ,odometer" +
                    "    ,total_fuel" +
                    "    ,device_serial_number" +
                    "    ,device_id" +
                    "    ,address_number" +
                    "    ,address_street_name" +
                    "    ,address_suburb" +
                    "    ,address_city" +
                    "    ,address_state" +
                    "    ,address_postal_code" +
                    "    ,address_country" +
                    "    ,address_timezone_offset" +
                    "    ,waypoint_set_point_id" +
                    "    ,waypoint_set_point_name" +
                    "    ,waypoint_external_ref" +
                    "    ,sequence_id" +
                    ")" +
                    "VALUES" +
                    "(" +
                    "       ?" +            // report_id
                    "      ,?" +            // fleet_id
                    "      ,?" +            // fleet_name
                    "      ,?" +            // vehicle_id
                    "      ,?" +            // vehicle_name
                    "      ,?" +            // vehicle_external_reference
                    "      ,?" +            // driver_id
                    "      ,?" +            // driver_name
                    "      ,?" +            // driver_external_reference
                    "      ,?" +            // gps_time
                    "      ,?" +            // device_time
                    "      ,?" +            // latitude
                    "      ,?" +            // longitude
                    "      ,?" +            // speed accumulation
                    "      ,?" +            // direction
                    "      ,?" +            // speed
                    "      ,?" +            // maximum_speed
                    "      ,?" +            // place_name
                    "      ,?" +            // map_reference
                    "      ,?" +            // hdop
                    "      ,?" +            // num_satellites_in_view
                    "      ,?" +            // altitude
                    "      ,?" +            // odometer
                    "      ,?" +            // total_fuel
                    "      ,?" +            // device_serial_number
                    "      ,?" +            // device_id
                    "      ,?" +            // address_number
                    "      ,?" +            // address_street_name
                    "      ,?" +            // address_suburb
                    "      ,?" +            // address_city
                    "      ,?" +            // address_state
                    "      ,?" +            // address_postal_code
                    "      ,?" +            // address_country
                    "      ,?" +            // address_timezone_offset
                    "      ,?" +            // waypoint_set_point_id
                    "      ,?" +            // waypoint_set_point_name
                    "      ,?" +            // waypoint_external_ref
                    "      ,?" +            // sequence_id"
                    ") ";

    static String upsertAssetLastPositionSql =
            "{call [MTDATA_KINESIS].[dbo].[upsertAssetLastPosition](" +
                    "?, ?, ?, ?, ?" +
                    ",? ,? ,?" +
                    ",?, ?, ?" +
                    ",?, ?, ?, ?, ?" +
                    ",?, ?, ?, ?, ?, ?" +
                    ",?, ?, ?, ?, ?" +
                    ")}"
            ;

    static String updateWatermarkSql =
            "MERGE INTO " +
                    "   [MTDATA_KINESIS].dbo.watermark wm " +
                    "USING " +
                    "   (VALUES ('events_logistics_raw', ?, SYSDATETIME())) AS source (tbl, last_id, last_update) " +
                    "ON " +
                    "   wm.table_name = source.tbl " +
                    "WHEN MATCHED THEN " +
                    "   UPDATE SET " +
                    "     last_id = source.last_id " +
                    "    ,last_update = source.last_update " +
                    "WHEN NOT MATCHED THEN " +
                    "   INSERT (table_name, last_id, last_update) " +
                    "   VALUES (source.tbl, source.last_id, source.last_update) " +
                    "; "
            ;


    static String jobActionCompleteSql =

            "INSERT INTO [MTDATA_KINESIS].[dbo].logistics_job_action_complete" +
                    "(" +
                     "    FleetId" +
                    "    ,FleetName" +
                    "    ,VehicleId" +
                    "    ,VehicleName" +
                    "    ,VehicleExternalRef" +
                    "    ,DriverId" +
                    "    ,DriverName" +
                    "    ,DriverExternalRef" +
                    ",ReportId"+
                    "    ,Odometer" +
                    "    ,TotalFuel" +
                    ",job_reference_type"+
                    ",job_reference_type_int"+
                    ",job_type"+
                    ",job_status"+
                    ",docket_number"+
                    ",leg_version"+
                    ",leg_id"+
                    ",leg_type"+
                    ",leg_status"+
                    ",leg_number"+
                    ",leg_title"+
                    ",leg_customer_group_id"+
                    ",leg_customer_group_version"+
                    ",leg_customer_id"+
                    ",leg_location_name"+
                    ",leg_address"+
                    ",leg_latitude"+
                    ",leg_longitude"+
                    ",leg_radius"+
                    ",leg_title"+
                    ",leg_title"+
                     ")" +
                    "VALUES" +
                    "(" +
                     "      ?" +            // fleet_id
                    "      ,?" +            // fleet_name
                    "      ,?" +            // vehicle_id
                    "      ,?" +            // vehicle_name
                    "      ,?" +            // vehicle_external_reference
                    "      ,?" +            // driver_id
                    "      ,?" +            // driver_name
                    "      ,?" +            // driver_external_reference
                    "      ,?" +            // gps_time
                     "      ,?" +            // latitude
                    "      ,?" +            // longitude
                    ") ";


}
