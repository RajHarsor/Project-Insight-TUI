import polars as pl
from datetime import datetime
from ..methods.initialize_methods import get_env_variables


def check_compliance(participant_id, 
                    start_date, 
                    end_date,
                    ):

    # Get the environment variables
    env_vars = get_env_variables()
    
    db_df = pl.read_csv(env_vars.get("participant_db"))
    survey_1a_df = pl.read_csv(env_vars.get("qualtrics_survey_1a_path"))
    survey_1b_df = pl.read_csv(env_vars.get("qualtrics_survey_1b_path"))
    survey_2_df = pl.read_csv(env_vars.get("qualtrics_survey_2_path"))
    survey_3_df = pl.read_csv(env_vars.get("qualtrics_survey_3_path"))
    survey_4_df = pl.read_csv(env_vars.get("qualtrics_survey_4_path"))

    return pl.DataFrame({
        "Date": [start_date, end_date],
        "Compliance": ["Yes", "No"]
    })
    