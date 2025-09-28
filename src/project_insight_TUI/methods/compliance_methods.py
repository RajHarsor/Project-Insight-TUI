from pydoc import doc
import polars as pl
from great_tables import GT, style, loc
from datetime import datetime, timedelta
from ..methods.initialize_methods import get_env_variables
import boto3
import pytz
import numpy as np
import re
import os
from pylatex import Document, Section, Subsection, Command, MiniPage
from pylatex.utils import italic, NoEscape
from pylatex.base_classes import Environment


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
        else:
            use_age = False
        print(f"Use age for disambiguation: {use_age}")
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

"""Compliance Report Generation Code"""

def generate_compliance_report(date: str, path: str):
    # Get environment variables
    env_vars = get_env_variables()
    print(env_vars)
    
    # Initialize a session using your AWS credentials
    Session = boto3.Session(
        aws_access_key_id=env_vars['aws_access_key_id'],
        aws_secret_access_key=env_vars['aws_secret_access_key'],
        region_name=env_vars['region']
    )
    
    dynamodb = Session.resource('dynamodb')
    table = dynamodb.Table(env_vars['table_name'])
    
    # Get Items
    response = table.scan()
    data = response['Items']
    df = pl.DataFrame(data)
    print("Initial DF:")
    print(df)
    
    # Datetime Vars
    curr_date_time = datetime.now().strftime("%Y-%m-%d @ %H:%M")
    report_name = f"Project_Insight_Report.pdf"
    date_obj = datetime.strptime(date, "%Y-%m-%d")
    date_obj_minus_1 = date_obj - timedelta(days=1)
    date_str_minus_1 = date_obj_minus_1.strftime("%Y-%m-%d")
    date_str = date_obj.strftime("%Y-%m-%d")
    
    # Filter currently in the study
    df = df.with_columns(pl.col('participant_id').cast(pl.Int64))
    df = df.filter(pl.col('participant_id') < 99)
    
    df = df.with_columns(
        pl.col("study_start_date").cast(pl.Utf8).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
        pl.col("study_end_date").cast(pl.Utf8).str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
    )
    
    filtered_df_active = df.filter(
        (pl.col("study_start_date") <= date_obj) & (pl.col("study_end_date") >= date_obj)
    )

    filtered_df_active_full = filtered_df_active.with_columns(
        days_in_study=(
            (pl.lit(date_obj) - pl.col("study_start_date"))
            .dt.total_days()
            .cast(pl.Int32) + 1
        )
    )

    filtered_df_active_full = filtered_df_active_full.sort(
        pl.col("participant_id")
    )
    
    filtered_df_active = filtered_df_active.with_columns(
        days_in_study=(
            (pl.lit(date_obj) - pl.col("study_start_date"))
            .dt.total_days()
            .cast(pl.Int32) + 1
        )
    )

    filtered_df_active = filtered_df_active.sort(
        pl.col("participant_id")
    )
    
    filtered_df_active = filtered_df_active.select(
        pl.col("participant_id").alias("Participant ID Active"),
        pl.col("study_start_date").alias("Study Start Date Active"),
        pl.col("study_end_date").alias("Study End Date Active"),
        pl.col("days_in_study").alias("Days in Study Active")
    )

    print("Filtered Active DF:")
    print(filtered_df_active)
    
    # Past Participants
    filtered_df_past = df.filter(
        (pl.col("study_end_date") < date_obj)
    )

    filtered_df_past = filtered_df_past.sort(
        pl.col("participant_id")
    )
    
    filtered_df_past = filtered_df_past.select(
        pl.col("participant_id").alias("Participant ID Past"),
        pl.col("study_start_date").alias("Study Start Date Past"),
        pl.col("study_end_date").alias("Study End Date Past")
    )

    print("Filtered Past DF:")
    print(filtered_df_past)
    
    # Upcoming Participants
    filtered_df_inactive = df.filter(
    (pl.col("study_start_date") > date_obj)
    )

    filtered_df_inactive = filtered_df_inactive.sort(
        pl.col("participant_id")
    )

    filtered_df_inactive = filtered_df_inactive.select(
        pl.col("participant_id").alias("Participant ID Inactive"),
        pl.col("study_start_date").alias("Study Start Date Inactive"),
        pl.col("study_end_date").alias("Study End Date Inactive")
    )
    print("Filtered Inactive DF:")
    print(filtered_df_inactive)
    
    # Join DFs
    final_df = pl.concat([filtered_df_inactive, filtered_df_active, filtered_df_past], how="horizontal")
    final_df = final_df.with_columns(
        pl.arange(1, final_df.height + 1).alias("Index")
    )
    final_df = final_df.with_columns(pl.col(pl.FLOAT_DTYPES).fill_nan(None))
    final_df = final_df.with_columns(pl.col(pl.Date).cast(pl.Utf8).fill_null(" "))
    final_df = final_df.with_columns(pl.col("^Participant ID.*$").cast(pl.Utf8).fill_null(" "))
    
    print("Final DF:")
    print(final_df)
    
    # Counts
    inactive_count = filtered_df_inactive.height
    active_count = filtered_df_active.height
    past_count = filtered_df_past.height
    
    # GT Table
    gt = (
        GT(final_df)
        .tab_header(
            title="Recruitment Report"
        )
        .tab_spanner(
            label = "Inactive ({})".format(inactive_count),
            columns = ["Participant ID Inactive", "Study Start Date Inactive", "Study End Date Inactive"]
        )
        .tab_spanner(
            label = "Active ({})".format(active_count),
            columns = ["Participant ID Active", "Study Start Date Active", "Study End Date Active", "Days in Study Active"]
        )
        .tab_spanner(
            label = "Past ({})".format(past_count),
            columns = ["Participant ID Past", "Study Start Date Past", "Study End Date Past"]
        )
        .cols_label(
            **{
                "Participant ID Inactive": "ID",
                "Study Start Date Inactive": "Start Date",
                "Study End Date Inactive": "End Date",
                "Participant ID Active": "ID",
                "Study Start Date Active": "Start Date",
                "Study End Date Active": "End Date",
                "Days in Study Active": "Days",
                "Participant ID Past": "ID",
                "Study Start Date Past": "Start Date",
                "Study End Date Past": "End Date"
            }
        )
        .cols_move_to_start("Index")
        .cols_align("center", columns=final_df.columns)
        .tab_style(
            style = style.fill(color = "gainsboro"),
            locations = loc.body(columns = ["Participant ID Inactive", "Study Start Date Inactive", "Study End Date Inactive"])
        )
        .tab_style(
            style = style.fill(color = "lightyellow"),
            locations = loc.body(columns = ["Participant ID Active", "Study Start Date Active", "Study End Date Active", "Days in Study Active"])
        )
        .tab_style(
            style = style.fill(color = "lightgreen"),
            locations = loc.body(columns = ["Participant ID Past", "Study Start Date Past", "Study End Date Past"])
        )
    )
    latex_table = gt.as_latex()

    early_bird_log_dataframes = {}
    standard_log_dataframes = {}
    night_owl_log_dataframes = {}

    early_bird_dfs, standard_dfs, night_owl_dfs, date_obj_minus_1 = get_log_events_all(Session, date_obj, early_bird_log_dataframes, standard_log_dataframes, night_owl_log_dataframes)
    
    # Go through early bird DF
    previous_times_early = []
    current_times_early = []
    
    for df_name, df in early_bird_dfs.items():
        print(f"DataFrame Name: {df_name}")
        current_df = df

        # Go through each item in the dataframe for 'firstEventTimestamp', if it contains date_str_minus_1, add to previous_times, else add to current_times. If it doesn't have either, put ' '.
        for row in current_df.iter_rows(named=True):  # Add named=True here
            timestamp = row['firstEventTimestamp']

            if date_str_minus_1 in timestamp:
                previous_times_early.append(timestamp)
            elif date_str in timestamp:
                current_times_early.append(timestamp)
            else:
                previous_times_early.append(' ')
                current_times_early.append(' ')

    while len(current_times_early) < len(previous_times_early):
        current_times_early.append(' ')
        
    for i in range(len(previous_times_early)):
        if previous_times_early[i] != ' ':
            try:
                dt = datetime.strptime(previous_times_early[i], "%Y-%m-%dT%H:%M:%S")
                previous_times_early[i] = dt.strftime("%H:%M")
            except ValueError:
                previous_times_early[i] = ' '
        if current_times_early[i] != ' ':
            try:
                dt = datetime.strptime(current_times_early[i], "%Y-%m-%dT%H:%M:%S")
                current_times_early[i] = dt.strftime("%H:%M")
            except ValueError:
                current_times_early[i] = ' '
        
    print("Previous and Current Early Bird Times:")
    print(previous_times_early, current_times_early)
    
    log_times_df_early = pl.DataFrame({
        f"{date_str_minus_1}": previous_times_early,
        f"{date_str}": current_times_early
    })
    
    print(log_times_df_early)
    
    # Go through standard DF
    previous_times_standard = []
    current_times_standard = []
    
    for df_name, df in standard_dfs.items():
        print(f"DataFrame Name: {df_name}")
        current_df = df

        # Go through each item in the dataframe for 'firstEventTimestamp', if it contains date_str_minus_1, add to previous_times, else add to current_times. If it doesn't have either, put ' '.
        for row in current_df.iter_rows(named=True):  # Add named=True here
            timestamp = row['firstEventTimestamp']

            if date_str_minus_1 in timestamp:
                previous_times_standard.append(timestamp)
            elif date_str in timestamp:
                current_times_standard.append(timestamp)
            else:
                previous_times_standard.append(' ')
                current_times_standard.append(' ')
    
    while len(current_times_standard) < len(previous_times_standard):
        current_times_standard.append(' ')
    
    for i in range(len(previous_times_standard)):
        if previous_times_standard[i] != ' ':
            try:
                dt = datetime.strptime(previous_times_standard[i], "%Y-%m-%dT%H:%M:%S")
                previous_times_standard[i] = dt.strftime("%H:%M")
            except ValueError:
                previous_times_standard[i] = ' '
        if current_times_standard[i] != ' ':
            try:
                dt = datetime.strptime(current_times_standard[i], "%Y-%m-%dT%H:%M:%S")
                current_times_standard[i] = dt.strftime("%H:%M")
            except ValueError:
                current_times_standard[i] = ' '
                
    print("Previous and Current Standard Times:")
    print(previous_times_standard, current_times_standard)
    
    log_times_standard_df = pl.DataFrame({
        f"{date_str_minus_1}": previous_times_standard,
        f"{date_str}": current_times_standard
    })
    
    print("Standard Log Times DF:")
    print(log_times_standard_df)
    
    # Go through night owl DF
    previous_times_night_owl = []
    current_times_night_owl = []
    
    for df_name, df in night_owl_dfs.items():
        print(f"DataFrame Name: {df_name}")
        current_df = df

        # Go through each item in the dataframe for 'firstEventTimestamp', if it contains date_str_minus_1, add to previous_times, else add to current_times. If it doesn't have either, put ' '.
        for row in current_df.iter_rows(named=True):  # Add named=True here
            timestamp = row['firstEventTimestamp']

            if date_str_minus_1 in timestamp:
                previous_times_night_owl.append(timestamp)
            elif date_str in timestamp:
                current_times_night_owl.append(timestamp)
            else:
                previous_times_night_owl.append(' ')
                current_times_night_owl.append(' ')
    
    while len(current_times_night_owl) < len(previous_times_night_owl):
        current_times_night_owl.append(' ')
    
    for i in range(len(previous_times_night_owl)):
        if previous_times_night_owl[i] != ' ':
            try:
                dt = datetime.strptime(previous_times_night_owl[i], "%Y-%m-%dT%H:%M:%S")
                previous_times_night_owl[i] = dt.strftime("%H:%M")
            except ValueError:
                previous_times_night_owl[i] = ' '
        if current_times_night_owl[i] != ' ':
            try:
                dt = datetime.strptime(current_times_night_owl[i], "%Y-%m-%dT%H:%M:%S")
                current_times_night_owl[i] = dt.strftime("%H:%M")
            except ValueError:
                current_times_night_owl[i] = ' '
    
    print("Previous and Current Night Owl Times:")
    print(previous_times_night_owl, current_times_night_owl)
    
    log_times_night_owl_df = pl.DataFrame({
        f"{date_str_minus_1}": previous_times_night_owl,
        f"{date_str}": current_times_night_owl
    })
    
    print("Night Owl Log Times DF:")
    print(log_times_night_owl_df)
    
    # Make Wide dataframes
    early_bird_long = log_times_df_early.with_row_index("Survey").melt(
        id_vars=["Survey"],
        value_vars=[f"{date_str_minus_1}", f"{date_str}"],
        variable_name='Date',
        value_name='Time'
    )

    early_bird_wide = early_bird_long.pivot(
        values='Time',
        index='Date',
        columns='Survey'
    ).with_columns(
        # Rename survey columns to be more descriptive
        pl.col('0').alias('S1'),
        pl.col('1').alias('S2'), 
        pl.col('2').alias('S3'),
        pl.col('3').alias('S4')
    ).select(
        pl.col('Date'),
        pl.col('S1'),
        pl.col('S2'),
        pl.col('S3'),
        pl.col('S4')
    )

    early_bird_wide

    early_bird_wide_latex = GT(early_bird_wide).as_latex()
    
    standard_long = log_times_standard_df.with_row_index("Survey").melt(
        id_vars=["Survey"],
        value_vars=[f"{date_str_minus_1}", f"{date_str}"],
        variable_name='Date',
        value_name='Time'
    )

    standard_wide = standard_long.pivot(
        values='Time',
        index='Date',
        columns='Survey'
    ).with_columns(
        # Rename survey columns to be more descriptive
        pl.col('0').alias('S1'),
        pl.col('1').alias('S2'), 
        pl.col('2').alias('S3'),
        pl.col('3').alias('S4')
    ).select(
        pl.col('Date'),
        pl.col('S1'),
        pl.col('S2'),
        pl.col('S3'),
        pl.col('S4')
    )

    standard_wide

    standard_wide_latex = GT(standard_wide).as_latex()
    
    night_owl_long = log_times_night_owl_df.with_row_index("Survey").melt(
        id_vars=["Survey"],
        value_vars=[f"{date_str_minus_1}", f"{date_str}"],
        variable_name='Date',
        value_name='Time'
    )

    # Step 2: Convert back to wide format with the desired structure
    night_owl_wide = night_owl_long.pivot(
        values='Time',
        index='Date',
        columns='Survey'
    ).with_columns(
        # Rename survey columns to be more descriptive
        pl.col('0').alias('S1'),
        pl.col('1').alias('S2'), 
        pl.col('2').alias('S3'),
        pl.col('3').alias('S4')
    ).select(
        pl.col('Date'),
        pl.col('S1'),
        pl.col('S2'),
        pl.col('S3'),
        pl.col('S4')
    )

    night_owl_wide

    print(night_owl_wide)

    night_owl_wide_latex = GT(night_owl_wide).as_latex()
    
    early_birds_dfs, standard_dfs, night_owl_dfs, date_obj_minus_1 = get_log_events_all(Session, date_obj, early_bird_log_dataframes, standard_log_dataframes, night_owl_log_dataframes)
    compliance_df = compliance_check_day_level(date_obj, filtered_df_active_full, date_str, early_bird_wide, standard_wide, night_owl_wide)

    compliance_output_df = compliance_df.select([
        "participant_id_number",
        "initials",
        "Age",
        "days_in_study",
        "survey_4_prev_day_compliance",
        "survey_1_compliance",
        "survey_2_compliance",
        "survey_3_compliance",
        "survey_4_compliance"
    ])
    
    compliance_output_df = compliance_output_df.with_columns(
        pl.concat_str(
            [
                pl.col("participant_id_number").cast(pl.Utf8),
                pl.lit(" ("),
                pl.col("days_in_study").cast(pl.Utf8),
                pl.lit(")"),
            ]
        ).alias("participant_id")
    ).drop("participant_id_number", "days_in_study")
    
    compliance_output_df = compliance_output_df.select([
        "participant_id",
        "initials",
        "Age",
        "survey_4_prev_day_compliance",
        "survey_1_compliance",
        "survey_2_compliance",
        "survey_3_compliance",
        "survey_4_compliance"
    ]) .rename({
        "participant_id": "ID # (Days in Study)",
        "initials": "Initials",
        "Age": "Age",
        "survey_4_prev_day_compliance": "S4 (Prev Day)",
        "survey_1_compliance": "S1",
        "survey_2_compliance": "S2",
        "survey_3_compliance": "S3",
        "survey_4_compliance": "S4"
    })

    two_missed = check_two_nrs_in_a_row(compliance_output_df)
    
    missing_lb_survey = []
    for row in compliance_output_df.iter_rows(named=True):
        days_in_study = int(row["ID # (Days in Study)"].split(" (")[1].replace(")", ""))
        if 5 <= days_in_study <= 12:
            if row["S1"] == "NR":
                print(f"Warning: Participant {row['ID # (Days in Study)']} is between days 5-12 and has NR for S1.")
                missing_lb_survey.append(row['ID # (Days in Study)'])
                
    compliance_output_gt = GT(compliance_output_df)
    compliance_output_final = compliance_output_gt.as_latex()

    # Generate PDF
    geometry_options = {"tmargin": "1cm", "lmargin": "1cm", "rmargin": "1cm", "bmargin": "2cm"}
    doc = Document(geometry_options=geometry_options, documentclass='article')

    doc.packages.append(Command('usepackage', 'graphicx'))
    doc.packages.append(Command('usepackage', 'booktabs'))
    doc.packages.append(Command('usepackage', 'longtable'))
    doc.packages.append(Command('usepackage', 'array'))
    doc.packages.append(Command('usepackage', 'pdflscape'))
    doc.packages.append(Command('usepackage', 'multicol'))
    # Use helvet for Helvetica font and sectsty to style sections
    doc.packages.append(Command('usepackage', 'helvet'))
    doc.packages.append(Command('usepackage', 'sectsty'))
    
    # Set default font to sans-serif and apply to all section types
    doc.preamble.append(NoEscape(r'\renewcommand{\familydefault}{\sfdefault}'))
    doc.preamble.append(NoEscape(r'\allsectionsfont{\sffamily}'))
    # Add title at the top
    doc.append(NoEscape(r'{\centering'))
    doc.append(Command('Large'))
    doc.append(Command('textbf', 'Project Insight Report'))
    doc.append(Command('\\\\'))
    doc.append(Command('normalsize'))
    doc.append(f'Report for {date_str}')
    doc.append(Command('\\\\'))
    doc.append(f'Generated on: {curr_date_time}')
    doc.append(Command('vspace', '0.5cm'))
    doc.append(NoEscape(r'\par}'))

    #doc.append(NoEscape(r'\raggedright'))

    # Clean and add table
    latex_table_clean = latex_table
    latex_table_clean = re.sub(r'\\begin\{table\}\[.*?\]', '', latex_table_clean)
    latex_table_clean = re.sub(r'\\end\{table\}', '', latex_table_clean)
    latex_table_clean = re.sub(r'\\caption\*?\{[^}]*\}\s*', '', latex_table_clean)
    latex_table_clean = re.sub(r'\\label\{[^}]*\}\s*', '', latex_table_clean)
    latex_table_clean = re.sub(r'\\centering\s*', '', latex_table_clean)
    latex_table_clean = re.sub(r'\\rmfamily\s*', '', latex_table_clean)


    with doc.create(Section('Recruitment Report', numbering=False)):
        doc.append(Command('centering'))  # Center only the table, not the section title

        doc.append(NoEscape(latex_table_clean))
    #doc.append(NoEscape(r'\end{landscape}'))

    def clean_latex(latex_string):
        cleaned = latex_string
        cleaned = re.sub(r'\\begin\{table\}\[.*?\]', '', cleaned)
        cleaned = re.sub(r'\\end\{table\}', '', cleaned)
        cleaned = re.sub(r'\\caption\*?\{[^}]*\}\s*', '', cleaned)
        cleaned = re.sub(r'\\label\{[^}]*\}\s*', '', cleaned)
        cleaned = re.sub(r'\\centering\s*', '', cleaned)
        cleaned = re.sub(r'\\rmfamily\s*', '', cleaned)
        return cleaned

    early_bird_wide_latex_clean = clean_latex(early_bird_wide_latex)
    standard_wide_latex_clean = clean_latex(standard_wide_latex)
    night_owl_wide_latex_clean = clean_latex(night_owl_wide_latex)

    doc.append(NoEscape(r'{\raggedright'))  # begin left-aligned group

    # Left Column with manual "Send Times" header
    doc.append(NoEscape(r'\begin{minipage}[t]{0.48\textwidth}'))
    doc.append(Command('Large'))
    doc.append(Command('vspace', '0.3cm'))
    doc.append(Command('textbf', 'Send Times'))
    doc.append(Command('normalsize'))
    doc.append(NoEscape(r'\par'))
    doc.append(Command('vspace', '0.2cm'))

    with doc.create(Subsection('Early Bird', numbering=False)):
        doc.append(NoEscape(early_bird_wide_latex_clean))
        doc.append(NoEscape(r'\par'))

    doc.append(Command('vspace', '0.2cm'))

    with doc.create(Subsection('Standard', numbering=False)):
        doc.append(NoEscape(standard_wide_latex_clean))
        doc.append(NoEscape(r'\par'))

    doc.append(Command('vspace', '0.2cm'))

    with doc.create(Subsection('Night Owl', numbering=False)):
        doc.append(NoEscape(night_owl_wide_latex_clean))
        doc.append(NoEscape(r'\par'))

    doc.append(NoEscape(r'\end{minipage}'))

    # Right Column with manual "Compliance" header
    doc.append(NoEscape(r'\hfill'))
    doc.append(NoEscape(r'\begin{minipage}[t]{0.48\textwidth}'))
    doc.append(Command('Large'))
    doc.append(Command('vspace', '0.3cm'))
    doc.append(Command('textbf', 'Contact List'))
    doc.append(Command('vspace', '0.6cm'))
    doc.append(Command('normalsize'))
    doc.append(NoEscape(r'\par')) 

    doc.append(NoEscape(r'{\large\textbf{Missing Two Consecutive NRs - ID (Day in Study):}}'))
    doc.append(NoEscape(r'\begin{itemize}'))
    if two_missed:
        # Create a nested itemize environment for the participants
        doc.append(NoEscape(r'\begin{itemize}'))
        for participant in two_missed:
            doc.append(NoEscape(rf'\item {participant}'))
        doc.append(NoEscape(r'\end{itemize}'))
    doc.append(NoEscape(r'\end{itemize}'))

    doc.append(Command('vspace', '0.2cm'))

    doc.append(NoEscape(r'{\large\textbf{Missing LB Survey (Days 5-12) - ID (Day in Study):}}'))
    doc.append(NoEscape(r'\begin{itemize}'))
    if missing_lb_survey:
        # Create a nested itemize environment for the participants
        doc.append(NoEscape(r'\begin{itemize}'))
        for participant in missing_lb_survey:
            doc.append(NoEscape(rf'\item {participant}'))
        doc.append(NoEscape(r'\end{itemize}'))
    doc.append(NoEscape(r'\end{itemize}'))


    doc.append(NoEscape(r'\par'))
    doc.append(Command('vspace', '0.3cm'))
    # Add contact list content here

    doc.append(NoEscape(r'\end{minipage}'))

    doc.append(NoEscape(r'\newpage'))
    # Compliance Section
    with doc.create(Section('Compliance Report (Active Participants at Date)', numbering=False)):
        doc.append(Command('centering'))  # Center only the table, not the section title

        compliance_output_final_clean = clean_latex(compliance_output_final)
        doc.append(NoEscape(compliance_output_final_clean))

        # Make a note about the compliance codes
        doc.append(NoEscape(r'\begin{itemize}'))
        doc.append(NoEscape(r'\textbf{SR\_C}: Single Response Compliant (completed within 1 hour of scheduled time); '))
        doc.append(NoEscape(r'\textbf{SR\_NC}: Single Response Late (completed but not within 1 hour of scheduled time); '))
        doc.append(NoEscape(r'\textbf{MR\_C}: Multiple Responses Compliant (at least one response within 1 hour of scheduled time); '))
        doc.append(NoEscape(r'\textbf{MR\_NC}: Multiple Responses Late (responses submitted but none within 1 hour of scheduled time); '))
        doc.append(NoEscape(r'\textbf{NR}: No Response (no response submitted)'))
        doc.append(NoEscape(r'\end{itemize}'))
        
        # Make a note to check "NR" values against the actual data
        doc.append(NoEscape(r'\textit{Note: Please verify "NR" values against actual survey data to ensure accuracy. Some "NRs" may be due to initials or age mismatches.}'))


    doc.append(NoEscape(r'\par}'))  # end left-aligned group


    # Generate PDF path/report_name/current date and time
    pdf_path = f"{path}/{report_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    os.makedirs("reports", exist_ok=True)


    try:
        doc.generate_pdf(pdf_path.replace('.pdf', ''), clean_tex=False)
        print(f"Report generated: {pdf_path}")
    except Exception as e:
        print(f"PDF generated with warnings: {pdf_path}")
        print(f"Warning: {e}")
    
    # Find the files that have the same prefix as pdf_path but end in things other than .pdf
    base_path = pdf_path.replace('.pdf', '')
    for file in os.listdir(path):
        if file.startswith(os.path.basename(base_path)) and not file.endswith('.pdf'):
            try:
                os.remove(os.path.join(path, file))
                print(f"Removed auxiliary file: {file}")
            except Exception as e:
                print(f"Could not remove file: {file}. Error: {e}")

"""Auxiliary Functions"""

def get_log_events_all(Session, date_obj, early_bird_log_dataframes, standard_log_dataframes, night_owl_log_dataframes):
    early_bird_list = ['/aws/lambda/early_bird_schedule_message1', 
                        '/aws/lambda/early_bird_schedule_message2',
                        '/aws/lambda/early_bird_schedule_message3',
                        '/aws/lambda/early_bird_schedule_message4']

    standard_list = ['/aws/lambda/standard_schedule_message1', 
                    '/aws/lambda/standard_schedule_message2',
                    '/aws/lambda/standard_schedule_message3',
                    '/aws/lambda/standard_schedule_message4']

    night_owl_list = ['/aws/lambda/night_owl_schedule_message1', 
                    '/aws/lambda/night_owl_schedule_message2',
                    '/aws/lambda/night_owl_schedule_message3',
                    '/aws/lambda/night_owl_schedule_message4']

    # date_obj - 1
    date_obj_minus_1 = date_obj - timedelta(days=1)

    cloudwatch_logs = Session.client('logs')

    # Early Bird
    for log_group_name in early_bird_list:
        print(log_group_name)
        response = cloudwatch_logs.describe_log_streams(
                logGroupName=log_group_name,
                orderBy='LogStreamName',
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

        log_stream_df = log_stream_df.filter(
            (pl.col('firstEventTimestamp').dt.date() == date_obj_minus_1.date()) |
            (pl.col('firstEventTimestamp').dt.date() == date_obj.date())
        )
        print(f"Start Date: {date_obj_minus_1}, End Date: {date_obj}")

        # Convert firstEventTimestamp to string for filtering
        log_stream_df = log_stream_df.with_columns(
            pl.col('firstEventTimestamp').dt.strftime("%Y-%m-%dT%H:%M:%S").alias('firstEventTimestamp')
        )

        early_bird_log_dataframes[log_group_name] = log_stream_df

    # Standard
    for log_group_name in standard_list:
        print(log_group_name)
        response = cloudwatch_logs.describe_log_streams(
                logGroupName=log_group_name,
                orderBy='LogStreamName',
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

        log_stream_df = log_stream_df.filter(
            (pl.col('firstEventTimestamp').dt.date() == date_obj_minus_1.date()) |
            (pl.col('firstEventTimestamp').dt.date() == date_obj.date())
        )
        print(f"Start Date: {date_obj_minus_1}, End Date: {date_obj}")

        # Convert firstEventTimestamp to string for filtering
        log_stream_df = log_stream_df.with_columns(
            pl.col('firstEventTimestamp').dt.strftime("%Y-%m-%dT%H:%M:%S").alias('firstEventTimestamp')
        )

        standard_log_dataframes[log_group_name] = log_stream_df

    # Night Owl
    for log_group_name in night_owl_list:
        print(log_group_name)
        response = cloudwatch_logs.describe_log_streams(
                logGroupName=log_group_name,
                orderBy='LogStreamName',
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

        log_stream_df = log_stream_df.filter(
            (pl.col('firstEventTimestamp').dt.date() == date_obj_minus_1.date()) |
            (pl.col('firstEventTimestamp').dt.date() == date_obj.date())
        )
        print(f"Start Date: {date_obj_minus_1}, End Date: {date_obj}")

        # Convert firstEventTimestamp to string for filtering
        log_stream_df = log_stream_df.with_columns(
            pl.col('firstEventTimestamp').dt.strftime("%Y-%m-%dT%H:%M:%S").alias('firstEventTimestamp')
        )

        night_owl_log_dataframes[log_group_name] = log_stream_df

    return early_bird_log_dataframes, standard_log_dataframes, night_owl_log_dataframes, date_obj_minus_1


def compliance_check_day_level(date_obj, filtered_df_active_full, date_str, early_bird_wide, standard_wide, night_owl_wide):
    env_vars = get_env_variables()
    
    now = datetime.now()
    is_today = date_obj.date() == now.date()
    
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
    
    # Find IDs
    merged_df = filtered_df_active_full.join(db_df, left_on="participant_id", right_on="Participant ID #", how="left")

    merged_df = merged_df.select(
        pl.col("participant_id").alias("participant_id_number"),
        pl.col("study_start_date"),
        pl.col("study_end_date"),
        pl.col("schedule_type"),
        pl.col("days_in_study"),
        pl.col("ID").alias("initials"),
        pl.col("Age")
    )
    
    merged_df = merged_df.with_columns(
        pl.lit("").alias("survey_4_prev_day_compliance"),
        pl.lit("").alias("survey_1_compliance"),
        pl.lit("").alias("survey_2_compliance"),
        pl.lit("").alias("survey_3_compliance"),
        pl.lit("").alias("survey_4_compliance")
    )
    
    for row in merged_df.iter_rows(named=True):
            participant_id = row["participant_id_number"]
            participant_age = row["Age"]
            participant_initials = row["initials"]
            schedule_type = row["schedule_type"]
            days_in_study = row["days_in_study"]

            # Check if anyone else has the same initials
            if merged_df.filter(pl.col("initials") == participant_initials).height > 1:
                use_age = True
            else:
                use_age = False

            # survey_1_compliance (Current Day)
            if (days_in_study >= 1 and days_in_study <= 4) or (days_in_study >=13 and days_in_study <=14):
                if use_age is True:
                        survey_1b_row = survey_1b_df.filter(
                            (pl.col("Date") == date_str) & 
                            ((pl.col("Name").str.to_lowercase() == participant_initials.lower())) &
                            (pl.col("Age") == int(participant_age))
                        )
                else:
                    survey_1b_row = survey_1b_df.filter(
                        (pl.col("Date") == date_str) & (pl.col("Name").str.to_lowercase() == participant_initials.lower())
                    )

                if not survey_1b_row.is_empty():
                    print(f"Participant {participant_id} completed Survey 1B on {date_str}")
                    if len(survey_1b_row) == 1:
                        time = survey_1b_row["Time"][0]

                        # Based on schedule_type, determine compliance
                        if schedule_type == "Early Bird Schedule":
                            #Check early_bird_wide for the time
                            eb_survey_1_time = early_bird_wide.filter(pl.col("Date") == date_str)["S1"][0]
                            if eb_survey_1_time != ' ':
                                # Check if the time is within 1 hour of the eb_survey_1_time (convert into datetime object)
                                eb_survey_1_datetime = datetime.strptime(f"{date_str} {eb_survey_1_time}", "%Y-%m-%d %H:%M")
                                survey_1b_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_1b_datetime - eb_survey_1_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_C"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                                else:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_NC"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                        if schedule_type == "Standard Schedule":
                            #Check standard_wide for the time
                            st_survey_1_time = standard_wide.filter(pl.col("Date") == date_str)["S1"][0]
                            if st_survey_1_time != ' ':
                                # Check if the time is within 1 hour of the st_survey_1_time (convert into datetime object)
                                st_survey_1_datetime = datetime.strptime(f"{date_str} {st_survey_1_time}", "%Y-%m-%d %H:%M")
                                survey_1b_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_1b_datetime - st_survey_1_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_C"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                                else:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_NC"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                        if schedule_type == "Night Owl Schedule":
                            #Check night_owl_wide for the time
                            no_survey_1_time = night_owl_wide.filter(pl.col("Date") == date_str)["S1"][0]
                            if no_survey_1_time != ' ':
                                # Check if the time is within 1 hour of the no_survey_1_time (convert into datetime object)
                                no_survey_1_datetime = datetime.strptime(f"{date_str} {no_survey_1_time}", "%Y-%m-%d %H:%M")
                                survey_1b_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_1b_datetime - no_survey_1_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_C"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                                else:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_NC"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                    if len(survey_1b_row) > 1:
                        time = survey_1b_row["Time"][0]
                        # Iterate through each row time and check if any are compliant (within 1 hour of scheduled time) based on schedule_type
                        compliant_found = False
                        for survey_time in survey_1b_row["Time"]:
                            if schedule_type == "Early Bird Schedule":
                                eb_survey_1_time = early_bird_wide.filter(pl.col("Date") == date_str)["S1"][0]
                                if eb_survey_1_time != ' ':
                                    eb_survey_1_datetime = datetime.strptime(f"{date_str} {eb_survey_1_time}", "%Y-%m-%d %H:%M")
                                    survey_1b_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                    time_diff = (survey_1b_datetime - eb_survey_1_datetime).total_seconds() / 60  # in minutes
                                    if 0 <= time_diff <= 60:
                                        merged_df = merged_df.with_columns(
                                            pl.when(pl.col("participant_id_number") == participant_id)
                                            .then(pl.lit("MR_NC"))
                                            .otherwise(pl.col("survey_1_compliance"))
                                            .alias("survey_1_compliance")
                                        )
                                        compliant_found = True
                                        break
                            if schedule_type == "Standard Schedule":
                                st_survey_1_time = standard_wide.filter(pl.col("Date") == date_str)["S1"][0]
                                if st_survey_1_time != ' ':
                                    st_survey_1_datetime = datetime.strptime(f"{date_str} {st_survey_1_time}", "%Y-%m-%d %H:%M")
                                    survey_1b_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                    time_diff = (survey_1b_datetime - st_survey_1_datetime).total_seconds() / 60  # in minutes
                                    if 0 <= time_diff <= 60:
                                        merged_df = merged_df.with_columns(
                                            pl.when(pl.col("participant_id_number") == participant_id)
                                            .then(pl.lit("MR_NC"))
                                            .otherwise(pl.col("survey_1_compliance"))
                                            .alias("survey_1_compliance")
                                        )
                                        compliant_found = True
                                        break
                            if schedule_type == "Night Owl Schedule":
                                no_survey_1_time = night_owl_wide.filter(pl.col("Date") == date_str)["S1"][0]
                                if no_survey_1_time != ' ':
                                    no_survey_1_datetime = datetime.strptime(f"{date_str} {no_survey_1_time}", "%Y-%m-%d %H:%M")
                                    survey_1b_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                    time_diff = (survey_1b_datetime - no_survey_1_datetime).total_seconds() / 60  # in minutes
                                    if 0 <= time_diff <= 60:
                                        merged_df = merged_df.with_columns(
                                            pl.when(pl.col("participant_id_number") == participant_id)
                                            .then(pl.lit("MR_NC"))
                                            .otherwise(pl.col("survey_1_compliance"))
                                            .alias("survey_1_compliance")
                                        )
                                        compliant_found = True
                                        break
                        if not compliant_found:
                            merged_df = merged_df.with_columns(
                                pl.when(pl.col("participant_id_number") == participant_id)
                                .then(pl.lit("MR_NC"))
                                .otherwise(pl.col("survey_1_compliance"))
                                .alias("survey_1_compliance")
                            )
                else:
                    # Check if the survey time has passed before marking as NR
                    should_mark_nr = True
                    if is_today:
                        scheduled_time_str = ''
                        if schedule_type == "Early Bird Schedule":
                            scheduled_time_str = early_bird_wide.filter(pl.col("Date") == date_str)["S1"][0]
                        elif schedule_type == "Standard Schedule":
                            scheduled_time_str = standard_wide.filter(pl.col("Date") == date_str)["S1"][0]
                        elif schedule_type == "Night Owl Schedule":
                            scheduled_time_str = night_owl_wide.filter(pl.col("Date") == date_str)["S1"][0]

                        if not scheduled_time_str or scheduled_time_str.isspace():
                            should_mark_nr = False
                        else:
                            scheduled_datetime = datetime.strptime(f"{date_str} {scheduled_time_str}", "%Y-%m-%d %H:%M")
                            if scheduled_datetime > now:
                                should_mark_nr = False

                    if should_mark_nr:
                        print(f"Participant {participant_id} did NOT complete Survey 1B on {date_str}")
                        merged_df = merged_df.with_columns(
                            pl.when(pl.col("participant_id_number") == participant_id)
                            .then(pl.lit("NR"))
                            .otherwise(pl.col("survey_1_compliance"))
                            .alias("survey_1_compliance")
                        )
            elif days_in_study >= 5 and days_in_study <= 12:
                if use_age is True:
                    survey_1a_row = survey_1a_df.filter(
                        (pl.col("Date") == date_str) & 
                        ((pl.col("Name").str.to_lowercase() == participant_initials.lower())) &
                        (pl.col("Age") == int(participant_age))
                    )
                else:
                    survey_1a_row = survey_1a_df.filter(
                        (pl.col("Date") == date_str) & (pl.col("Name").str.to_lowercase() == participant_initials.lower())
                    )

                if not survey_1a_row.is_empty():
                    print(f"Participant {participant_id} completed Survey 1A on {date_str}")
                    if len(survey_1a_row) == 1:
                        time = survey_1a_row["Time"][0]

                        # Based on schedule_type, determine compliance
                        if schedule_type == "Early Bird Schedule":
                            #Check early_bird_wide for the time
                            eb_survey_1_time = early_bird_wide.filter(pl.col("Date") == date_str)["S1"][0]
                            if eb_survey_1_time != ' ':
                                # Check if the time is within 1 hour of the eb_survey_1_time (convert into datetime object)
                                eb_survey_1_datetime = datetime.strptime(f"{date_str} {eb_survey_1_time}", "%Y-%m-%d %H:%M")
                                survey_1a_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_1a_datetime - eb_survey_1_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_C"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                                else:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_NC"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                        if schedule_type == "Standard Schedule":
                            #Check standard_wide for the time
                            st_survey_1_time = standard_wide.filter(pl.col("Date") == date_str)["S1"][0]
                            if st_survey_1_time != ' ':
                                # Check if the time is within 1 hour of the st_survey_1_time (convert into datetime object)
                                st_survey_1_datetime = datetime.strptime(f"{date_str} {st_survey_1_time}", "%Y-%m-%d %H:%M")
                                survey_1a_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_1a_datetime - st_survey_1_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_C"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                                else:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_NC"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                        if schedule_type == "Night Owl Schedule":
                            #Check night_owl_wide for the time
                            no_survey_1_time = night_owl_wide.filter(pl.col("Date") == date_str)["S1"][0]
                            if no_survey_1_time != ' ':
                                # Check if the time is within 1 hour of the no_survey_1_time (convert into datetime object)
                                no_survey_1_datetime = datetime.strptime(f"{date_str} {no_survey_1_time}", "%Y-%m-%d %H:%M")
                                survey_1a_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_1a_datetime - no_survey_1_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_C"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                                else:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("SR_NC"))
                                        .otherwise(pl.col("survey_1_compliance"))
                                        .alias("survey_1_compliance")
                                    )
                    if len(survey_1a_row) > 1:
                        time = survey_1a_row["Time"][0]
                        # Iterate through each row time and check if any are compliant (within 1 hour of scheduled time) based on schedule_type
                        compliant_found = False
                        for survey_time in survey_1a_row["Time"]:
                            if schedule_type == "Early Bird Schedule":
                                eb_survey_1_time = early_bird_wide.filter(pl.col("Date") == date_str)["S1"][0]
                                if eb_survey_1_time != ' ':
                                    eb_survey_1_datetime = datetime.strptime(f"{date_str} {eb_survey_1_time}", "%Y-%m-%d %H:%M")
                                    survey_1a_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                    time_diff = (survey_1a_datetime - eb_survey_1_datetime).total_seconds() / 60  # in minutes
                                    if 0 <= time_diff <= 60:
                                        merged_df = merged_df.with_columns(
                                            pl.when(pl.col("participant_id_number") == participant_id)
                                            .then(pl.lit("MR_NC"))
                                            .otherwise(pl.col("survey_1_compliance"))
                                            .alias("survey_1_compliance")
                                        )
                                        compliant_found = True
                                        break
                            if schedule_type == "Standard Schedule":
                                st_survey_1_time = standard_wide.filter(pl.col("Date") == date_str)["S1"][0]
                                if st_survey_1_time != ' ':
                                    st_survey_1_datetime = datetime.strptime(f"{date_str} {st_survey_1_time}", "%Y-%m-%d %H:%M")
                                    survey_1a_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                    time_diff = (survey_1a_datetime - st_survey_1_datetime).total_seconds() / 60  # in minutes
                                    if 0 <= time_diff <= 60:
                                        merged_df = merged_df.with_columns(
                                            pl.when(pl.col("participant_id_number") == participant_id)
                                            .then(pl.lit("MR_NC"))
                                            .otherwise(pl.col("survey_1_compliance"))
                                            .alias("survey_1_compliance")
                                        )
                                        compliant_found = True
                                        break
                            if schedule_type == "Night Owl Schedule":
                                no_survey_1_time = night_owl_wide.filter(pl.col("Date") == date_str)["S1"][0]
                                if no_survey_1_time != ' ':
                                    no_survey_1_datetime = datetime.strptime(f"{date_str} {no_survey_1_time}", "%Y-%m-%d %H:%M")
                                    survey_1a_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                    time_diff = (survey_1a_datetime - no_survey_1_datetime).total_seconds() / 60  # in minutes
                                    if 0 <= time_diff <= 60:
                                        merged_df = merged_df.with_columns(
                                            pl.when(pl.col("participant_id_number") == participant_id)
                                            .then(pl.lit("MR_NC"))
                                            .otherwise(pl.col("survey_1_compliance"))
                                            .alias("survey_1_compliance")
                                        )
                                        compliant_found = True
                                        break
                        if not compliant_found:
                            merged_df = merged_df.with_columns(
                                pl.when(pl.col("participant_id_number") == participant_id)
                                .then(pl.lit("MR_NC"))
                                .otherwise(pl.col("survey_1_compliance"))
                                .alias("survey_1_compliance")
                            )
                else:
                    # Check if the survey time has passed before marking as NR
                    should_mark_nr = True
                    if is_today:
                        scheduled_time_str = ''
                        if schedule_type == "Early Bird Schedule":
                            scheduled_time_str = early_bird_wide.filter(pl.col("Date") == date_str)["S1"][0]
                        elif schedule_type == "Standard Schedule":
                            scheduled_time_str = standard_wide.filter(pl.col("Date") == date_str)["S1"][0]
                        elif schedule_type == "Night Owl Schedule":
                            scheduled_time_str = night_owl_wide.filter(pl.col("Date") == date_str)["S1"][0]

                        if not scheduled_time_str or scheduled_time_str.isspace():
                            should_mark_nr = False
                        else:
                            scheduled_datetime = datetime.strptime(f"{date_str} {scheduled_time_str}", "%Y-%m-%d %H:%M")
                            if scheduled_datetime > now:
                                should_mark_nr = False

                    if should_mark_nr:
                        print(f"Participant {participant_id} did NOT complete Survey 1A on {date_str}")
                        merged_df = merged_df.with_columns(
                            pl.when(pl.col("participant_id_number") == participant_id)
                            .then(pl.lit("NR"))
                            .otherwise(pl.col("survey_1_compliance"))
                            .alias("survey_1_compliance")
                        )

            # survey_2_compliance (Current Day)
            if use_age is True:
                survey_2_row = survey_2_df.filter(
                    (pl.col("Date") == date_str) & 
                    ((pl.col("Name").str.to_lowercase() == participant_initials.lower())) &
                    (pl.col("Age") == int(participant_age))
                )
            else:
                survey_2_row = survey_2_df.filter(
                    (pl.col("Date") == date_str) & (pl.col("Name").str.to_lowercase() == participant_initials.lower())
                )

            if not survey_2_row.is_empty():
                print(f"Participant {participant_id} completed Survey 2 on {date_str}")
                if len(survey_2_row) == 1:
                    time = survey_2_row["Time"][0]

                    # Based on schedule_type, determine compliance
                    if schedule_type == "Early Bird Schedule":
                        #Check early_bird_wide for the time
                        eb_survey_2_time = early_bird_wide.filter(pl.col("Date") == date_str)["S2"][0]
                        if eb_survey_2_time != ' ':
                            # Check if the time is within 1 hour of the eb_survey_2_time (convert into datetime object)
                            eb_survey_2_datetime = datetime.strptime(f"{date_str} {eb_survey_2_time}", "%Y-%m-%d %H:%M")
                            survey_2_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_2_datetime - eb_survey_2_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_2_compliance"))
                                    .alias("survey_2_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_2_compliance"))
                                    .alias("survey_2_compliance")
                                )
                    if schedule_type == "Standard Schedule":
                        #Check standard_wide for the time
                        st_survey_2_time = standard_wide.filter(pl.col("Date") == date_str)["S2"][0]
                        if st_survey_2_time != ' ':
                            # Check if the time is within 1 hour of the st_survey_2_time (convert into datetime object)
                            st_survey_2_datetime = datetime.strptime(f"{date_str} {st_survey_2_time}", "%Y-%m-%d %H:%M")
                            survey_2_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_2_datetime - st_survey_2_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_2_compliance"))
                                    .alias("survey_2_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_2_compliance"))
                                    .alias("survey_2_compliance")
                                )
                    if schedule_type == "Night Owl Schedule":
                        #Check night_owl_wide for the time
                        no_survey_2_time = night_owl_wide.filter(pl.col("Date") == date_str)["S2"][0]
                        if no_survey_2_time != ' ':
                            # Check if the time is within 1 hour of the no_survey_2_time (convert into datetime object)
                            no_survey_2_datetime = datetime.strptime(f"{date_str} {no_survey_2_time}", "%Y-%m-%d %H:%M")
                            survey_2_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_2_datetime - no_survey_2_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_2_compliance"))
                                    .alias("survey_2_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_2_compliance"))
                                    .alias("survey_2_compliance")
                                )
                if len(survey_2_row) > 1:
                    time = survey_2_row["Time"][0]
                    # Iterate through each row time and check if any are compliant (within 1 hour of scheduled time) based on schedule_type
                    compliant_found = False
                    for survey_time in survey_2_row["Time"]:
                        if schedule_type == "Early Bird Schedule":
                            eb_survey_2_time = early_bird_wide.filter(pl.col("Date") == date_str)["S2"][0]
                            if eb_survey_2_time != ' ':
                                eb_survey_2_datetime = datetime.strptime(f"{date_str} {eb_survey_2_time}", "%Y-%m-%d %H:%M")
                                survey_2_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_2_datetime - eb_survey_2_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_C"))
                                        .otherwise(pl.col("survey_2_compliance"))
                                        .alias("survey_2_compliance")
                                    )
                                    compliant_found = True
                                    break
                        if schedule_type == "Standard Schedule":
                            st_survey_2_time = standard_wide.filter(pl.col("Date") == date_str)["S2"][0]
                            if st_survey_2_time != ' ':
                                st_survey_2_datetime = datetime.strptime(f"{date_str} {st_survey_2_time}", "%Y-%m-%d %H:%M")
                                survey_2_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_2_datetime - st_survey_2_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_C"))
                                        .otherwise(pl.col("survey_2_compliance"))
                                        .alias("survey_2_compliance")
                                    )
                                    compliant_found = True
                                    break
                        if schedule_type == "Night Owl Schedule":
                            no_survey_2_time = night_owl_wide.filter(pl.col("Date") == date_str)["S2"][0]
                            if no_survey_2_time != ' ':
                                no_survey_2_datetime = datetime.strptime(f"{date_str} {no_survey_2_time}", "%Y-%m-%d %H:%M")
                                survey_2_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_2_datetime - no_survey_2_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_C"))
                                        .otherwise(pl.col("survey_2_compliance"))
                                        .alias("survey_2_compliance")
                                    )
                                    compliant_found = True
                                    break
                    if not compliant_found:
                        merged_df = merged_df.with_columns(
                            pl.when(pl.col("participant_id_number") == participant_id)
                            .then(pl.lit("MR_NC"))
                            .otherwise(pl.col("survey_2_compliance"))
                            .alias("survey_2_compliance")
                        )
            else:
                # Check if the survey time has passed before marking as NR
                should_mark_nr = True
                if is_today:
                    scheduled_time_str = ''
                    if schedule_type == "Early Bird Schedule":
                        scheduled_time_str = early_bird_wide.filter(pl.col("Date") == date_str)["S2"][0]
                    elif schedule_type == "Standard Schedule":
                        scheduled_time_str = standard_wide.filter(pl.col("Date") == date_str)["S2"][0]
                    elif schedule_type == "Night Owl Schedule":
                        scheduled_time_str = night_owl_wide.filter(pl.col("Date") == date_str)["S2"][0]

                    if not scheduled_time_str or scheduled_time_str.isspace():
                        should_mark_nr = False
                    else:
                        scheduled_datetime = datetime.strptime(f"{date_str} {scheduled_time_str}", "%Y-%m-%d %H:%M")
                        if scheduled_datetime > now:
                            should_mark_nr = False

                if should_mark_nr:
                    print(f"Participant {participant_id} did NOT complete Survey 2 on {date_str}")
                    merged_df = merged_df.with_columns(
                        pl.when(pl.col("participant_id_number") == participant_id)
                        .then(pl.lit("NR"))
                        .otherwise(pl.col("survey_2_compliance"))
                        .alias("survey_2_compliance")
                    )

            # survey_3_compliance (Current Day)
            if use_age is True:
                survey_3_row = survey_3_df.filter(
                    (pl.col("Date") == date_str) & 
                    ((pl.col("Name").str.to_lowercase() == participant_initials.lower())) &
                    (pl.col("Age") == int(participant_age))
                )
            else:
                survey_3_row = survey_3_df.filter(
                    (pl.col("Date") == date_str) & (pl.col("Name").str.to_lowercase() == participant_initials.lower())
                )

            if not survey_3_row.is_empty():
                print(f"Participant {participant_id} completed Survey 3 on {date_str}")
                if len(survey_3_row) == 1:
                    time = survey_3_row["Time"][0]

                    # Based on schedule_type, determine compliance
                    if schedule_type == "Early Bird Schedule":
                        #Check early_bird_wide for the time
                        eb_survey_3_time = early_bird_wide.filter(pl.col("Date") == date_str)["S3"][0]
                        if eb_survey_3_time != ' ':
                            # Check if the time is within 1 hour of the eb_survey_3_time (convert into datetime object)
                            eb_survey_3_datetime = datetime.strptime(f"{date_str} {eb_survey_3_time}", "%Y-%m-%d %H:%M")
                            survey_3_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_3_datetime - eb_survey_3_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_3_compliance"))
                                    .alias("survey_3_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_3_compliance"))
                                    .alias("survey_3_compliance")
                                )
                    if schedule_type == "Standard Schedule":
                        #Check standard_wide for the time
                        st_survey_3_time = standard_wide.filter(pl.col("Date") == date_str)["S3"][0]
                        if st_survey_3_time != ' ':
                            # Check if the time is within 1 hour of the st_survey_3_time (convert into datetime object)
                            st_survey_3_datetime = datetime.strptime(f"{date_str} {st_survey_3_time}", "%Y-%m-%d %H:%M")
                            survey_3_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_3_datetime - st_survey_3_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_3_compliance"))
                                    .alias("survey_3_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_3_compliance"))
                                    .alias("survey_3_compliance")
                                )
                    if schedule_type == "Night Owl Schedule":
                        #Check night_owl_wide for the time
                        no_survey_3_time = night_owl_wide.filter(pl.col("Date") == date_str)["S3"][0]
                        if no_survey_3_time != ' ':
                            # Check if the time is within 1 hour of the no_survey_3_time (convert into datetime object)
                            no_survey_3_datetime = datetime.strptime(f"{date_str} {no_survey_3_time}", "%Y-%m-%d %H:%M")
                            survey_3_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_3_datetime - no_survey_3_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_3_compliance"))
                                    .alias("survey_3_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_3_compliance"))
                                    .alias("survey_3_compliance")
                                )
                if len(survey_3_row) > 1:
                    time = survey_3_row["Time"][0]
                    # Iterate through each row time and check if any are compliant (within 1 hour of scheduled time) based on schedule_type
                    compliant_found = False
                    for survey_time in survey_3_row["Time"]:
                        if schedule_type == "Early Bird Schedule":
                            eb_survey_3_time = early_bird_wide.filter(pl.col("Date") == date_str)["S3"][0]
                            if eb_survey_3_time != ' ':
                                eb_survey_3_datetime = datetime.strptime(f"{date_str} {eb_survey_3_time}", "%Y-%m-%d %H:%M")
                                survey_3_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_3_datetime - eb_survey_3_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_NC"))
                                        .otherwise(pl.col("survey_3_compliance"))
                                        .alias("survey_3_compliance")
                                    )
                                    compliant_found = True
                                    break
                        if schedule_type == "Standard Schedule":
                            st_survey_3_time = standard_wide.filter(pl.col("Date") == date_str)["S3"][0]
                            if st_survey_3_time != ' ':
                                st_survey_3_datetime = datetime.strptime(f"{date_str} {st_survey_3_time}", "%Y-%m-%d %H:%M")
                                survey_3_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_3_datetime - st_survey_3_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_NC"))
                                        .otherwise(pl.col("survey_3_compliance"))
                                        .alias("survey_3_compliance")
                                    )
                                    compliant_found = True
                                    break
                        if schedule_type == "Night Owl Schedule":
                            no_survey_3_time = night_owl_wide.filter(pl.col("Date") == date_str)["S3"][0]
                            if no_survey_3_time != ' ':
                                no_survey_3_datetime = datetime.strptime(f"{date_str} {no_survey_3_time}", "%Y-%m-%d %H:%M")
                                survey_3_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_3_datetime - no_survey_3_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_NC"))
                                        .otherwise(pl.col("survey_3_compliance"))
                                        .alias("survey_3_compliance")
                                    )
                                    compliant_found = True
                                    break
                    if not compliant_found:
                        merged_df = merged_df.with_columns(
                            pl.when(pl.col("participant_id_number") == participant_id)
                            .then(pl.lit("MR_NC"))
                            .otherwise(pl.col("survey_3_compliance"))
                            .alias("survey_3_compliance")
                        )
                        compliant_found = True
                        break
            else:
                # Check if the survey time has passed before marking as NR
                should_mark_nr = True
                if is_today:
                    scheduled_time_str = ''
                    if schedule_type == "Early Bird Schedule":
                        scheduled_time_str = early_bird_wide.filter(pl.col("Date") == date_str)["S3"][0]
                    elif schedule_type == "Standard Schedule":
                        scheduled_time_str = standard_wide.filter(pl.col("Date") == date_str)["S3"][0]
                    elif schedule_type == "Night Owl Schedule":
                        scheduled_time_str = night_owl_wide.filter(pl.col("Date") == date_str)["S3"][0]

                    if not scheduled_time_str or scheduled_time_str.isspace():
                        should_mark_nr = False
                    else:
                        scheduled_datetime = datetime.strptime(f"{date_str} {scheduled_time_str}", "%Y-%m-%d %H:%M")
                        if scheduled_datetime > now:
                            should_mark_nr = False

                if should_mark_nr:
                    print(f"Participant {participant_id} did NOT complete Survey 3 on {date_str}")
                    merged_df = merged_df.with_columns(
                        pl.when(pl.col("participant_id_number") == participant_id)
                        .then(pl.lit("NR"))
                        .otherwise(pl.col("survey_3_compliance"))
                        .alias("survey_3_compliance")
                    )

            # survey_4_compliance (Current Day)
            if use_age is True:
                survey_4_row = survey_4_df.filter(
                    (pl.col("Date") == date_str) & 
                    ((pl.col("Name").str.to_lowercase() == participant_initials.lower())) &
                    (pl.col("Age") == int(participant_age))
                )
            else:
                survey_4_row = survey_4_df.filter(
                    (pl.col("Date") == date_str) & (pl.col("Name").str.to_lowercase() == participant_initials.lower())
                )
            if not survey_4_row.is_empty():
                print(f"Participant {participant_id} completed Survey 4 on {date_str}")
                if len(survey_4_row) == 1:
                    time = survey_4_row["Time"][0]

                    # Based on schedule_type, determine compliance
                    if schedule_type == "Early Bird Schedule":
                        #Check early_bird_wide for the time
                        eb_survey_4_time = early_bird_wide.filter(pl.col("Date") == date_str)["S4"][0]
                        if eb_survey_4_time != ' ':
                            # Check if the time is within 1 hour of the eb_survey_4_time (convert into datetime object)
                            eb_survey_4_datetime = datetime.strptime(f"{date_str} {eb_survey_4_time}", "%Y-%m-%d %H:%M")
                            survey_4_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_4_datetime - eb_survey_4_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_4_compliance"))
                                    .alias("survey_4_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_4_compliance"))
                                    .alias("survey_4_compliance")
                                )
                    if schedule_type == "Standard Schedule":
                        #Check standard_wide for the time
                        st_survey_4_time = standard_wide.filter(pl.col("Date") == date_str)["S4"][0]
                        if st_survey_4_time != ' ':
                            # Check if the time is within 1 hour of the st_survey_4_time (convert into datetime object)
                            st_survey_4_datetime = datetime.strptime(f"{date_str} {st_survey_4_time}", "%Y-%m-%d %H:%M")
                            survey_4_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_4_datetime - st_survey_4_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_4_compliance"))
                                    .alias("survey_4_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_4_compliance"))
                                    .alias("survey_4_compliance")
                                )
                    if schedule_type == "Night Owl Schedule":
                        #Check night_owl_wide for the time
                        no_survey_4_time = night_owl_wide.filter(pl.col("Date") == date_str)["S4"][0]
                        if no_survey_4_time != ' ':
                            # Check if the time is within 1 hour of the no_survey_4_time (convert into datetime object)
                            no_survey_4_datetime = datetime.strptime(f"{date_str} {no_survey_4_time}", "%Y-%m-%d %H:%M")
                            survey_4_datetime = datetime.strptime(f"{date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_4_datetime - no_survey_4_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_4_compliance"))
                                    .alias("survey_4_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_4_compliance"))
                                    .alias("survey_4_compliance")
                                )
                if len(survey_4_row) > 1:
                    time = survey_4_row["Time"][0]
                    # Iterate through each row time and check if any are compliant (within 1 hour of scheduled time) based on schedule_type
                    compliant_found = False
                    for survey_time in survey_4_row["Time"]:
                        if schedule_type == "Early Bird Schedule":
                            eb_survey_4_time = early_bird_wide.filter(pl.col("Date") == date_str)["S4"][0]
                            if eb_survey_4_time != ' ':
                                eb_survey_4_datetime = datetime.strptime(f"{date_str} {eb_survey_4_time}", "%Y-%m-%d %H:%M")
                                survey_4_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_4_datetime - eb_survey_4_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_C"))
                                        .otherwise(pl.col("survey_4_compliance"))
                                        .alias("survey_4_compliance")
                                    )
                                    compliant_found = True
                                    break
                        if schedule_type == "Standard Schedule":
                            st_survey_4_time = standard_wide.filter(pl.col("Date") == date_str)["S4"][0]
                            if st_survey_4_time != ' ':
                                st_survey_4_datetime = datetime.strptime(f"{date_str} {st_survey_4_time}", "%Y-%m-%d %H:%M")
                                survey_4_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_4_datetime - st_survey_4_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_C"))
                                        .otherwise(pl.col("survey_4_compliance"))
                                        .alias("survey_4_compliance")
                                    )
                                    compliant_found = True
                                    break
                        if schedule_type == "Night Owl Schedule":
                            no_survey_4_time = night_owl_wide.filter(pl.col("Date") == date_str)["S4"][0]
                            if no_survey_4_time != ' ':
                                no_survey_4_datetime = datetime.strptime(f"{date_str} {no_survey_4_time}", "%Y-%m-%d %H:%M")
                                survey_4_datetime = datetime.strptime(f"{date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_4_datetime - no_survey_4_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_C"))
                                        .otherwise(pl.col("survey_4_compliance"))
                                        .alias("survey_4_compliance")
                                    )
                                    compliant_found = True
                                    break
                    if not compliant_found:
                        merged_df = merged_df.with_columns(
                            pl.when(pl.col("participant_id_number") == participant_id)
                            .then(pl.lit("MR_NC"))
                            .otherwise(pl.col("survey_4_compliance"))
                            .alias("survey_4_compliance")
                        )
            else:
                # Check if the survey time has passed before marking as NR
                should_mark_nr = True
                if is_today:
                    scheduled_time_str = ''
                    if schedule_type == "Early Bird Schedule":
                        scheduled_time_str = early_bird_wide.filter(pl.col("Date") == date_str)["S4"][0]
                    elif schedule_type == "Standard Schedule":
                        scheduled_time_str = standard_wide.filter(pl.col("Date") == date_str)["S4"][0]
                    elif schedule_type == "Night Owl Schedule":
                        scheduled_time_str = night_owl_wide.filter(pl.col("Date") == date_str)["S4"][0]

                    if not scheduled_time_str or scheduled_time_str.isspace():
                        should_mark_nr = False
                    else:
                        scheduled_datetime = datetime.strptime(f"{date_str} {scheduled_time_str}", "%Y-%m-%d %H:%M")
                        if scheduled_datetime > now:
                            should_mark_nr = False

                if should_mark_nr:
                    print(f"Participant {participant_id} did NOT complete Survey 4 on {date_str}")
                    merged_df = merged_df.with_columns(
                        pl.when(pl.col("participant_id_number") == participant_id)
                        .then(pl.lit("NR"))
                        .otherwise(pl.col("survey_4_compliance"))
                        .alias("survey_4_compliance")
                    )

            # survey_4_prev_day_compliance
            prev_date_str = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
            if use_age is True:
                survey_4_prev_row = survey_4_df.filter(
                    (pl.col("Date") == prev_date_str) & 
                    ((pl.col("Name").str.to_lowercase() == participant_initials.lower())) &
                    (pl.col("Age") == int(participant_age))
                )
            else:
                survey_4_prev_row = survey_4_df.filter(
                    (pl.col("Date") == prev_date_str) & (pl.col("Name").str.to_lowercase() == participant_initials.lower())
                )
            if not survey_4_prev_row.is_empty():
                print(f"Participant {participant_id} completed Survey 4 on {prev_date_str}")
                if len(survey_4_prev_row) == 1:
                    time = survey_4_prev_row["Time"][0]

                    # Based on schedule_type, determine compliance
                    if schedule_type == "Early Bird Schedule":
                        #Check early_bird_wide for the time
                        eb_survey_4_time = early_bird_wide.filter(pl.col("Date") == prev_date_str)["S4"][0]
                        if eb_survey_4_time != ' ':
                            # Check if the time is within 1 hour of the eb_survey_4_time (convert into datetime object)
                            eb_survey_4_datetime = datetime.strptime(f"{prev_date_str} {eb_survey_4_time}", "%Y-%m-%d %H:%M")
                            survey_4_datetime = datetime.strptime(f"{prev_date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_4_datetime - eb_survey_4_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_4_prev_day_compliance"))
                                    .alias("survey_4_prev_day_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_4_prev_day_compliance"))
                                    .alias("survey_4_prev_day_compliance")
                                )
                    elif schedule_type == "Standard Schedule":
                        #Check standard_wide for the time
                        st_survey_4_time = standard_wide.filter(pl.col("Date") == prev_date_str)["S4"][0]
                        if st_survey_4_time != ' ':
                            # Check if the time is within 1 hour of the st_survey_4_time (convert into datetime object)
                            st_survey_4_datetime = datetime.strptime(f"{prev_date_str} {st_survey_4_time}", "%Y-%m-%d %H:%M")
                            survey_4_datetime = datetime.strptime(f"{prev_date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_4_datetime - st_survey_4_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_4_prev_day_compliance"))
                                    .alias("survey_4_prev_day_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_4_prev_day_compliance"))
                                    .alias("survey_4_prev_day_compliance")
                                )
                    elif schedule_type == "Night Owl Schedule":
                        #Check night_owl_wide for the time
                        no_survey_4_time = night_owl_wide.filter(pl.col("Date") == prev_date_str)["S4"][0]
                        if no_survey_4_time != ' ':
                            # Check if the time is within 1 hour of the no_survey_4_time (convert into datetime object)
                            no_survey_4_datetime = datetime.strptime(f"{prev_date_str} {no_survey_4_time}", "%Y-%m-%d %H:%M")
                            survey_4_datetime = datetime.strptime(f"{prev_date_str} {time}", "%Y-%m-%d %H:%M:%S")
                            time_diff = (survey_4_datetime - no_survey_4_datetime).total_seconds() / 60  # in minutes
                            if 0 <= time_diff <= 60:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_C"))
                                    .otherwise(pl.col("survey_4_prev_day_compliance"))
                                    .alias("survey_4_prev_day_compliance")
                                )
                            else:
                                merged_df = merged_df.with_columns(
                                    pl.when(pl.col("participant_id_number") == participant_id)
                                    .then(pl.lit("SR_NC"))
                                    .otherwise(pl.col("survey_4_prev_day_compliance"))
                                    .alias("survey_4_prev_day_compliance")
                                )
                if len(survey_4_prev_row) > 1:
                    time = survey_4_prev_row["Time"][0]
                    # Iterate through each row time and check if any are compliant (within 1 hour of scheduled time) based on schedule_type
                    compliant_found = False
                    for survey_time in survey_4_prev_row["Time"]:
                        if schedule_type == "Early Bird Schedule":
                            eb_survey_4_time = early_bird_wide.filter(pl.col("Date") == prev_date_str)["S4"][0]
                            if eb_survey_4_time != ' ':
                                eb_survey_4_datetime = datetime.strptime(f"{prev_date_str} {eb_survey_4_time}", "%Y-%m-%d %H:%M")
                                survey_4_datetime = datetime.strptime(f"{prev_date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_4_datetime - eb_survey_4_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_NC"))
                                        .otherwise(pl.col("survey_4_prev_day_compliance"))
                                        .alias("survey_4_prev_day_compliance")
                                    )
                                    compliant_found = True
                                    break
                        elif schedule_type == "Standard Schedule":
                            st_survey_4_time = standard_wide.filter(pl.col("Date") == prev_date_str)["S4"][0]
                            if st_survey_4_time != ' ':
                                st_survey_4_datetime = datetime.strptime(f"{prev_date_str} {st_survey_4_time}", "%Y-%m-%d %H:%M")
                                survey_4_datetime = datetime.strptime(f"{prev_date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_4_datetime - st_survey_4_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_NC"))
                                        .otherwise(pl.col("survey_4_prev_day_compliance"))
                                        .alias("survey_4_prev_day_compliance")
                                    )
                                    compliant_found = True
                                    break
                        elif schedule_type == "Night Owl Schedule":
                            no_survey_4_time = night_owl_wide.filter(pl.col("Date") == prev_date_str)["S4"][0]
                            if no_survey_4_time != ' ':
                                no_survey_4_datetime = datetime.strptime(f"{prev_date_str} {no_survey_4_time}", "%Y-%m-%d %H:%M")
                                survey_4_datetime = datetime.strptime(f"{prev_date_str} {survey_time}", "%Y-%m-%d %H:%M:%S")
                                time_diff = (survey_4_datetime - no_survey_4_datetime).total_seconds() / 60  # in minutes
                                if 0 <= time_diff <= 60:
                                    merged_df = merged_df.with_columns(
                                        pl.when(pl.col("participant_id_number") == participant_id)
                                        .then(pl.lit("MR_NC"))
                                        .otherwise(pl.col("survey_4_prev_day_compliance"))
                                        .alias("survey_4_prev_day_compliance")
                                    )
                                    compliant_found = True
                                    break
                    if not compliant_found:
                        merged_df = merged_df.with_columns(
                            pl.when(pl.col("participant_id_number") == participant_id)
                            .then(pl.lit("MR_NC"))
                            .otherwise(pl.col("survey_4_prev_day_compliance"))
                            .alias("survey_4_prev_day_compliance")
                        )
                        compliant_found = True
                        break
            else:
                print(f"Participant {participant_id} did NOT complete Survey 4 on {prev_date_str}")
                merged_df = merged_df.with_columns(
                    pl.when(pl.col("participant_id_number") == participant_id)
                    .then(pl.lit("NR"))
                    .otherwise(pl.col("survey_4_prev_day_compliance"))
                    .alias("survey_4_prev_day_compliance")
                )
    return merged_df

def check_two_nrs_in_a_row(df):
    two_missed = []
    for row in df.iter_rows(named=True):
        # Check the compliance columns for two consecutive NRs
        compliance_values = [
            row["S4 (Prev Day)"],
            row["S1"],
            row["S2"],
            row["S3"],
            row["S4"]
        ]
        for i in range(len(compliance_values) - 1):
            if compliance_values[i] == "NR" and compliance_values[i + 1] == "NR":
                print(f"Warning: Participant {row['ID # (Days in Study)']} has two consecutive NRs in their compliance data.")
                two_missed.append(row['ID # (Days in Study)'])
                break
    return two_missed
