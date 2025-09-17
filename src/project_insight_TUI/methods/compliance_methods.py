import polars as pl
from datetime import datetime
from ..methods.initialize_methods import get_env_variables
import boto3
import datetime
import pytz
import numpy as np


def get_participant_variables(participant_id: str):
    env_vars = get_env_variables()
    
    Session = boto3.Session(
        aws_access_key_id=env_vars['aws_access_key_id'],
        aws_secret_access_key=env_vars['aws_secret_access_key'],
        region_name=env_vars['region']
    )
    
    # Get the needed variables
    dynamodb = Session.resource('dynamodb')
    table = dynamodb.Table(env_vars['table_name'])

    response = table.get_item(Key={"participant_id": participant_id})
    
    study_start_date = response['Item']['study_start_date']
    study_end_date = response['Item']['study_end_date']
    schedule_type = response['Item']['schedule_type']

    
    return study_start_date, study_end_date, schedule_type

def get_log_events(schedule_type, date_range, study_start_date_converted, study_end_date_converted):
    
    if schedule_type == "Early Bird Schedule":
        log_group_name_list = ['/aws/lambda/early_bird_schedule_message1', 
                               '/aws/lambda/early_bird_schedule_message2',
                               '/aws/lambda/early_bird_schedule_message3',
                               '/aws/lambda/early_bird_schedule_message4']
    elif schedule_type == "Standard Schedule":
        log_group_name_list = ['/aws/lambda/standard_schedule_message1', 
                               '/aws/lambda/standard_schedule_message2',
                               '/aws/lambda/standard_schedule_message3',
                               '/aws/lambda/standard_schedule_message4']
    elif schedule_type == "Night Owl Schedule":
        log_group_name_list = ['/aws/lambda/night_owl_schedule_message1', 
                               '/aws/lambda/night_owl_schedule_message2',
                               '/aws/lambda/night_owl_schedule_message3',
                               '/aws/lambda/night_owl_schedule_message4']
    else:
        raise ValueError("Invalid schedule type provided.")
    
    env_vars = get_env_variables()
    
    Session = boto3.Session(
        aws_access_key_id=env_vars['aws_access_key_id'],
        aws_secret_access_key=env_vars['aws_secret_access_key'],
        region_name=env_vars['region']
    )
    
    # Create a CloudWatch Logs client
    cloudwatch_logs = Session.client('logs')
    
    send_time_dict = {date: [] for date in date_range}
    for log_group_name in log_group_name_list:
        response = cloudwatch_logs.describe_log_streams(
            logGroupName=log_group_name,
            orderBy='LastEventTime',
            descending=True
        )
        log_stream_df = pl.DataFrame(response['logStreams'])
        
        log_stream_df = log_stream_df.with_columns(
            pl.from_epoch(pl.col('firstEventTimestamp'), time_unit="ms").alias('firstEventTimestamp'),
            pl.from_epoch(pl.col('lastEventTimestamp'), time_unit="ms").alias('lastEventTimestamp'),
            pl.from_epoch(pl.col('creationTime'), time_unit="ms").alias('creationTime')
        )
        log_stream_df = log_stream_df.with_columns(
            pl.col('firstEventTimestamp').dt.replace_time_zone("UTC").dt.convert_time_zone("America/New_York").alias('firstEventTimestamp'),
            pl.col('lastEventTimestamp').dt.replace_time_zone("UTC").dt.convert_time_zone("America/New_York").alias('lastEventTimestamp'),
            pl.col('creationTime').dt.replace_time_zone("UTC").dt.convert_time_zone("America/New_York").alias('creationTime')
        )
        
        print(f"Processing log group_unaltered: {log_group_name}")
        print(log_stream_df)
        
        
        # Only include log streams where the firstEventTimestamp is between the study start and end dates
        log_stream_df = log_stream_df.filter(
            (pl.col('firstEventTimestamp') >= study_start_date_converted) &
            (pl.col('firstEventTimestamp') <= study_end_date_converted)
        )
        print(f"Start Date: {study_start_date_converted}, End Date: {study_end_date_converted}")
        
        # Convert firstEventTimestamp to string for filtering
        log_stream_df = log_stream_df.with_columns(
            pl.col('firstEventTimestamp').dt.strftime("%Y-%m-%dT%H:%M:%S").alias('firstEventTimestamp')
        )

        
        print(f"Processing log group: {log_group_name}")
        with pl.Config(tbl_rows=-1, tbl_cols=-1):
            print(log_stream_df)
        # For each date, find the firstEventTimestamp for this log group
        for date in date_range:
            match = None
            for row in log_stream_df.iter_rows(named=True):
                # Check if the timestamp string starts with the date (e.g., '2025-08-12')
                if date in row['firstEventTimestamp']:
                    match = row
                    break
            if match and match['firstEventTimestamp'] is not None:
                # firstEventTimestamp is already a string in "%Y-%m-%dT%H:%M:%S" format
                time_str = match['firstEventTimestamp'][11:19]  # Extract "HH:MM:SS"
                send_time_dict[date].append(time_str)
            else:
                send_time_dict[date].append(None)
    return send_time_dict

def generate_compliance_tables(participant_id: str):
    env_vars = get_env_variables()
    print(env_vars)
    
    # Initialize a session using your AWS credentials
    Session = boto3.Session(
        aws_access_key_id=env_vars['aws_access_key_id'],
        aws_secret_access_key=env_vars['aws_secret_access_key'],
        region_name=env_vars['region']
    )
    
    # Load in Survey CSV Files & Clean Times
    try:
        db_df = pl.read_csv(env_vars.get("participant_db_path"))
        survey_1a_df = pl.read_csv(env_vars.get("qualtrics_survey_1a_path"), schema_overrides={"Date/Time": str})
        survey_1b_df = pl.read_csv(env_vars.get("qualtrics_survey_1b_path"), schema_overrides={"Date/Time": str})
        survey_2_df = pl.read_csv(env_vars.get("qualtrics_survey_2_path"), schema_overrides={"Date/Time": str})
        survey_3_df = pl.read_csv(env_vars.get("qualtrics_survey_3_path"), schema_overrides={"Date/Time": str})
        survey_4_df = pl.read_csv(env_vars.get("qualtrics_survey_4_path"), schema_overrides={"Date/Time": str})
        
        survey_list = [survey_1a_df, survey_1b_df, survey_2_df, survey_3_df, survey_4_df]
        
        print("CSV files loaded successfully.")
    except Exception as e:
        print(f"Error loading CSV files: {e}")
        message = f"Error loading CSV files: {e}. Please check if you have Google Drive open and are logged in."
        return None, None, None, message, None, None, None
    
    for idx, survey in enumerate(survey_list):
        survey = survey.with_columns(
        pl.col("Date/Time").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).alias("Date/Time")
        )
        
        survey = survey.with_columns(
        pl.col("Date/Time").dt.replace_time_zone("America/Denver").dt.convert_time_zone("America/New_York").alias("Date/Time")
        ) 
        
        survey = survey.with_columns(
        pl.col("Date/Time").dt.strftime("%Y-%m-%d").alias("Date"),
        pl.col("Date/Time").dt.strftime("%H:%M:%S").alias("Time")
        )
        
        # Remove whitespace from any entry in "Name" column
        survey = survey.with_columns(
            pl.col("Name").str.strip_chars().alias("Name"),
        )
        
        survey_list[idx] = survey  # Update the original list
    
    survey_1a_df = survey_list[0]
    survey_1b_df = survey_list[1]
    survey_2_df = survey_list[2]
    survey_3_df = survey_list[3]
    survey_4_df = survey_list[4]
    
    # Get participant variables
    study_start_date, study_end_date, schedule_type = get_participant_variables(str(participant_id))
    
    ## Convert dates to datetime objects
    study_start_date_converted = datetime.datetime.strptime(study_start_date, "%Y-%m-%d").replace(tzinfo=pytz.timezone("America/New_York"))
    study_end_date_converted = datetime.datetime.strptime(study_end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=pytz.timezone("America/New_York")
    )
    
    # Date Range
    date_range = []
    current_date = study_start_date_converted
    while current_date <= study_end_date_converted:
        date_range.append(current_date.strftime("%Y-%m-%d"))
        current_date += datetime.timedelta(days=1)
    
    # Get log events
    dict = get_log_events(schedule_type, date_range, study_start_date_converted, study_end_date_converted)
    send_time_dict = {date: [datetime.datetime.strptime(time, "%H:%M:%S") if time else None for time in times] for date, times in dict.items()}


    participant_row = db_df.filter(pl.col("Participant ID #") == int(participant_id))
    print("Participant Row:")
    print(participant_row)
    # ID Number Identification
    try:
        ID = str(participant_row["ID"][0])
        print(f"Participant ID {participant_id} corresponds to initials: {ID}")
        age = str(participant_row["Age"][0])
        
        # Check if an ID is the same as another ID in the database
        if db_df.filter(pl.col("ID") == ID).height > 1:
            use_age = True
    except Exception as e:
        print(f"Error retrieving participant initials: {e}")
        message = f"Error retrieving participant initials: {e}"
        return None, None, None, message, None, None, None
    
    # Setup for checks
    list_of_len = list(range(1, len(date_range) + 1))
    zip_date_strings = list(zip(date_range, list_of_len))
    participant_completion_times_dict = {study_day: [[],[],[],[]] for study_day in list_of_len}

    # Populate the participant_completion_times_dict with times from each survey
    for i in zip_date_strings:
        day = int(i[1])
        date = i[0]
        date_datetime = datetime.datetime.strptime(date, "%Y-%m-%d")
        curr_date = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # Only check the days that have happened so far and today
        if date_datetime <= datetime.datetime.strptime(curr_date, "%Y-%m-%d"):
            print(f"\nChecking date: {date} with day: {day}")
            
            # Check if the day is within the range of 1 to 4
            if (day >= 1 and day <= 4) or (day >= 13 and day <= 14):
                if use_age is True:
                    survey_1b_row = survey_1b_df.filter(
                        (pl.col("Date") == date) & 
                        ((pl.col("Name").str.to_lowercase() == ID.lower())) &
                        (pl.col("Age") == int(age))
                    )
                else:
                    survey_1b_row = survey_1b_df.filter(
                        (pl.col("Date") == date) & (pl.col("Name").str.to_lowercase() == ID.lower())
                    )
                
                if not survey_1b_row.is_empty():
                    print("  ✓ Found Survey 1b Row:", survey_1b_row)
                    # If length of survey_1b_row is 1, then append the time to the participant_completion_times_dict
                    if len(survey_1b_row) == 1:
                        date = survey_1b_row["Date"][0]
                        time = survey_1b_row["Time"][0]
                        name = survey_1b_row["Name"][0]
                        number_of_responses = len(survey_1b_row)
                        participant_completion_times_dict[day][0].append([time, name, date, number_of_responses, "S1b"])
                        print(f"  ✓ Appended time {time} to participant_completion_times_dict for day {day}")
                    elif len(survey_1b_row) > 1:
                        print(f"  ✗ More than one response found for date: {date}, ID: {ID}")
                        date = date
                        time = "Multiple Responses"
                        name = "Multiple Responses"
                        number_of_responses = len(survey_1b_row)
                        participant_completion_times_dict[day][0].append([time, name, date, number_of_responses, "S1b"])
                else:
                    print(f"  ✗ No match found for date: {date}, ID: {ID}")
                    participant_completion_times_dict[day][0].append(["No Response", "No Response", date, 0, "S1b"])
                
            elif day >= 5 and day <= 12:
                if use_age is True:
                    survey_1a_row = survey_1a_df.filter(
                        (pl.col("Date") == date) & 
                        ((pl.col("Name").str.to_lowercase() == ID.lower())) &
                        (pl.col("Age") == int(age))
                    )
                else:
                    survey_1a_row = survey_1a_df.filter(
                        (pl.col("Date") == date) & (pl.col("Name").str.to_lowercase() == ID.lower())
                    )
                    
                if not survey_1a_row.is_empty():
                    print("  ✓ Found Survey 1A Row:", survey_1a_row)
                    if len(survey_1a_row) == 1:
                        date = survey_1a_row["Date"][0]
                        time = survey_1a_row["Time"][0]
                        name = survey_1a_row["Name"][0]
                        number_of_responses = len(survey_1a_row)
                        participant_completion_times_dict[day][0].append([time, name, date, number_of_responses, "S1a"])
                    elif len(survey_1a_row) > 1:
                        print(f"  ✗ More than one response found for date: {date}, ID: {ID}")
                        date = date
                        time = "Multiple Responses"
                        name = "Multiple Responses"
                        number_of_responses = len(survey_1a_row)
                        participant_completion_times_dict[day][0].append([time, name, date, number_of_responses, "S1a"])
                else:
                    print(f"  ✗ No match found for date: {date}, ID: {ID}")
                    participant_completion_times_dict[day][0].append(["No Response", "No Response", date, 0, "S1a"])
            else:
                print(f"  Day {day} is not in any expected range")
        
        # Check survey 2 (no specific day range, just check if the date exists)
        if date_datetime <= datetime.datetime.strptime(curr_date, "%Y-%m-%d"):
            if use_age is True:
                survey_2_row = survey_2_df.filter(
                    (pl.col("Date") == date) & 
                    ((pl.col("Name").str.to_lowercase() == ID.lower())) &
                    (pl.col("Age") == int(age))
                )
            else:
                survey_2_row = survey_2_df.filter(
                    (pl.col("Date") == date) & (pl.col("Name").str.to_lowercase() == ID.lower())
                )
                
            if not survey_2_row.is_empty():
                print("  ✓ Found Survey 2 Row:", survey_2_row)
                if len(survey_2_row) == 1:
                    date = survey_2_row["Date"][0]
                    time = survey_2_row["Time"][0]
                    name = survey_2_row["Name"][0]
                    number_of_responses = len(survey_2_row)
                    participant_completion_times_dict[day][1].append([time, name, date, number_of_responses, "S2"])
                elif len(survey_2_row) > 1:
                    print(f"  ✗ More than one response found for date: {date}, ID: {ID}")
                    date = date
                    time = "Multiple Responses"
                    name = "Multiple Responses"
                    number_of_responses = len(survey_2_row)
                    participant_completion_times_dict[day][1].append([time, name, date, number_of_responses, "S2"])
            else:
                print(f"  ✗ No match found for date: {date}, ID: {ID}")
                participant_completion_times_dict[day][1].append(["No Response", "No Response", date, 0, "S2"])

            
        # Check survey 3 (no specific day range, just check if the date exists)
        if date_datetime <= datetime.datetime.strptime(curr_date, "%Y-%m-%d"):
            if use_age is True:
                survey_3_row = survey_3_df.filter(
                    (pl.col("Date") == date) & 
                    ((pl.col("Name").str.to_lowercase() == ID.lower())) &
                    (pl.col("Age") == int(age))
                )
            else:
                survey_3_row = survey_3_df.filter(
                    (pl.col("Date") == date) & (pl.col("Name").str.to_lowercase() == ID.lower())
                )
            if not survey_3_row.is_empty():
                print("  ✓ Found Survey 3 Row:", survey_3_row)
                if len(survey_3_row) == 1:
                    date = survey_3_row["Date"][0]
                    time = survey_3_row["Time"][0]
                    name = survey_3_row["Name"][0]
                    number_of_responses = len(survey_3_row)
                    participant_completion_times_dict[day][2].append([time, name, date, number_of_responses, "S3"])
                elif len(survey_3_row) > 1:
                    print(f"  ✗ More than one response found for date: {date}, ID: {ID}")
                    date = date
                    time = "Multiple Responses"
                    name = "Multiple Responses"
                    number_of_responses = len(survey_3_row)
                    participant_completion_times_dict[day][2].append([time, name, date, number_of_responses, "S3"])
            else:
                print(f"  ✗ No match found for date: {date}, ID: {ID}")
                participant_completion_times_dict[day][2].append(["No Response", "No Response", date, 0, "S3"])
        
        # Check survey 4 (no specific day range, just check if the date exists)
            if date_datetime <= datetime.datetime.strptime(curr_date, "%Y-%m-%d"):
                if use_age is True:
                    survey_4_row = survey_4_df.filter(
                        (pl.col("Date") == date) & 
                        ((pl.col("Name").str.to_lowercase() == ID.lower())) &
                        (pl.col("Age") == int(age))
                    )
                else:
                    survey_4_row = survey_4_df.filter(
                        (pl.col("Date") == date) & (pl.col("Name").str.to_lowercase() == ID.lower())
                    )
                if not survey_4_row.is_empty():
                    print("  ✓ Found Survey 4 Row:", survey_4_row)
                    if len(survey_4_row) == 1:
                        date = survey_4_row["Date"][0]
                        time = survey_4_row["Time"][0]
                        name = survey_4_row["Name"][0]
                        number_of_responses = len(survey_4_row)
                        participant_completion_times_dict[day][3].append([time, name, date, number_of_responses, "S4"])
                    elif len(survey_4_row) > 1:
                        print(f"  ✗ More than one response found for date: {date}, ID: {ID}")
                        date = date
                        time = "Multiple Responses"
                        name = "Multiple Responses"
                        number_of_responses = len(survey_4_row)
                        participant_completion_times_dict[day][3].append([time, name, date, number_of_responses, "S4"])
                else:
                    print(f"  ✗ No match found for date: {date}, ID: {ID}")
                    participant_completion_times_dict[day][3].append(["No Response", "No Response", date, 0, "S4"])
                    
    # Convert the participant_completion_times_dict to a numpy array for easier manipulation
    dif_array = np.full((14,4), "", dtype=object)
    for responses in participant_completion_times_dict.keys():
        current_response = participant_completion_times_dict[responses]
        print(f"Day {responses}:")
        for survey_index, survey_responses in enumerate(current_response):
            print(f"  Survey {survey_index + 1} Responses:")
            for response in survey_responses:
                if response[0] == "No Response":
                    print(f"    No Response on {response[2]}")
                    # Store None in the dif_array
                    dif_array[responses - 1][survey_index] = "NR"
                elif response[0] == "Multiple Responses":
                    print(f"    Multiple Responses on {response[2]}")
                    date = response[2]
                    name = response[1]
                    survey_type = response[4]
                    
                    match survey_type:
                        case "S1a":
                            survey_name = survey_1a_df
                        case "S1b":
                            survey_name = survey_1b_df
                        case "S2":
                            survey_name = survey_2_df
                        case "S3":
                            survey_name = survey_3_df
                        case "S4":
                            survey_name = survey_4_df
                    
                    filtered_rows = survey_name.filter(
                        (pl.col("Date") == date) & (pl.col("Name").str.to_lowercase() == ID.lower())
                    )
                    print(filtered_rows)
                    
                    send_time = send_time_dict[date][survey_index]
                    # Convert the Time column in the filtered_rows to datetime.time
                    filtered_rows = filtered_rows.with_columns(
                        pl.col("Time").str.strptime(pl.Time, "%H:%M:%S", strict=False).alias("Time")
                    )
                    # If the message hasn't been sent yet, leave blank and skip
                    if send_time is None:
                        print(f"    No send time for {date} Survey {survey_index + 1}; leaving blank.")
                        dif_array[responses - 1][survey_index] = ""
                        continue
                    for row in filtered_rows.iter_rows():
                        # See if the time is within 1 hour of the send time by calculating the difference
                        time_obj = row[-1]  # Assuming the Time column is at the last index
                        if send_time and time_obj:
                            if isinstance(time_obj, str):
                                time_obj = datetime.datetime.strptime(time_obj, "%H:%M:%S").time()
                            # Combine date and time to create datetime objects
                            date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")
                            response_dt = datetime.datetime.combine(date_obj, time_obj)
                            send_dt = datetime.datetime.combine(date_obj, send_time.time())
                            difference = (response_dt - send_dt).total_seconds() / 3600  # Convert to hours
                            print(f"    Comparing send time {send_dt} with response time {response_dt}, difference: {difference} hours")
                            if difference <= 1:
                                print(f"    Time matches for {response[2]}: {send_dt} == {response_dt}")
                                dif_array[responses - 1][survey_index] = "✓ MR"
                                break  # Exit the loop after finding a match
                            else:
                                print(f"    Time does not match for {response[2]}: {send_dt} != {response_dt}")
                                dif_array[responses - 1][survey_index] = "✗ MR"
                    
                # Check if it's blank - if it is put 0
                elif response[0] is None or response[0] == "":
                        print(f"    Date not occured yet on {response[2]}")
                        # Store None in the dif_array
                        dif_array[responses - 1][survey_index] = 0
                else:
                    print(f"    Response on {response[2]}: {response[0]}")
                    time = response[0]
                    date = response[2]
                    name = response[1]
                    survey_type = response[4]
                    
                    match survey_type:
                        case "S1a":
                            survey_name = survey_1a_df
                        case "S1b":
                            survey_name = survey_1b_df
                        case "S2":
                            survey_name = survey_2_df
                        case "S3":
                            survey_name = survey_3_df
                        case "S4":
                            survey_name = survey_4_df
                    
                    # See if the time is within 1 hour of the send time
                    send_time = send_time_dict[date][survey_index]
                    # If the message hasn't been sent yet, leave blank and skip
                    if send_time is None:
                        print(f"    No send time for {date} Survey {survey_index + 1}; leaving blank.")
                        dif_array[responses - 1][survey_index] = ""
                        continue
                     
                    # Convert 'time' variable to a datetime.time object
                    if isinstance(time, str):
                        time_obj = datetime.datetime.strptime(time, "%H:%M:%S").time()
                    
                    # Calculate the difference between send_time and time_obj
                    date_obj = datetime.datetime.strptime(date, "%Y-%m-%d")
                    response_dt = datetime.datetime.combine(date_obj, time_obj)
                    send_dt = datetime.datetime.combine(date_obj, send_time.time())
                    difference = (response_dt - send_dt).total_seconds() / 3600  # Convert to hours
                    print(f"    Comparing send time {send_dt} with response time {response_dt}, difference: {difference} hours")
                    if difference <= 1:
                        print(f"    Time matches for {response[2]}: {send_dt} == {response_dt}")
                        dif_array[responses - 1][survey_index] = "✓ SR"
                    else:
                        print(f"    Time does not match for {response[2]}: {send_dt} != {response_dt}")
                        dif_array[responses - 1][survey_index] = "✗ SR"
    print(dif_array)
    
    # Count how many ✓ SR and ✓ MR
    count_mr = np.sum(dif_array == "✓ MR")
    count_sr = np.sum(dif_array == "✓ SR")
    total_comp = round(((count_mr + count_sr) / 56) * 100, 2)
    print("Total Completion Rate (✓ MR + ✓ SR):", total_comp)
    
    # Count how many not None in the dictionary
    count_not_none = sum(1 for times in send_time_dict.values() for time in times if time is not None)
    print("Count of None values in send_time_dict:", count_not_none)
    
    current_comp = round(((count_mr + count_sr) / count_not_none) * 100, 2) 
    print("Current Completion Rate (✓ MR + ✓ SR) / Total Sent:", current_comp)
    
    # Final rows to send to the frontend
    compliance_rows = []
    for i in range(len(dif_array)):
        row = dif_array[i]
        compliance_row = (
            i + 1,  # Day
            row[0],  # Survey 1A
            row[1],  # Survey 2
            row[2],  # Survey 3
            row[3]   # Survey 4
        )
        compliance_rows.append(compliance_row)
    compliance_rows.insert(0, ("Day", "Survey 1", "Survey 2", "Survey 3", "Survey 4"))
    print(compliance_rows)
    
    send_time_rows = []
    for i in range(14):
        date = date_range[i]
        times = dict[date]
        row = (
            date,
            i + 1,  # Day (1-indexed)
            times[0] if len(times) > 0 else None,
            times[1] if len(times) > 1 else None,
            times[2] if len(times) > 2 else None,
            times[3] if len(times) > 3 else None,
        )
        send_time_rows.append(row)
    send_time_rows.insert(0, ("Date", "Day", "Survey 1", "Survey 2", "Survey 3", "Survey 4"))
    message = "Quick note: \n In the compliance table you may see NR for surveys that have not been sent out yet for today or have not passed the hour long threshold participants have to complete the survey. This is a bug, though, the compliance calculations should still correct."
    #FIXME: One way to easily fix this bug is to compare the send_times_row with the compliance_rows. If send_time_row is None for that position, the compliance_row should be "" instead of NR or any other value.
    
    print(send_time_rows)
    
    print(f"dictionary: {dict}")
            
    return compliance_rows, send_time_rows, ID, message, current_comp, total_comp, age

