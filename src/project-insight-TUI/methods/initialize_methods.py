import os
from dotenv import load_dotenv

# Function to see if there is a .env file in the current directory
def check_env_file_exists() -> bool:
    load_dotenv()  # Load environment variables from .env file if it exists
    return os.path.exists('.env')  # Check if .env file exists in the current directory

def check_env_variables() -> bool:
    """
    Check if the required environment variables are set in the .env file only.
    Returns True if all required variables are set, otherwise False.
    """
    # Read the .env file directly to check only those variables
    env_vars = {}
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
    
    required_vars = ['aws_access_key_id', 'aws_secret_access_key', 'region', 'table_name']
    for var in required_vars:
        if var not in env_vars or not env_vars[var]:
            print(f"Environment variable '{var}' is not set in .env file.")
            return False
    return True  # All required environment variables are set in .env file

def create_env_file(aws_access_key_id: str,
                    aws_secret_access_key: str,
                    table_name: str,
                    qualtrics_survey_1a_path: str = None,
                    qualtrics_survey_1b_path: str = None,
                    qualtrics_survey_2_path: str = None,
                    qualtrics_survey_3_path: str = None,
                    qualtrics_survey_4_path: str = None):
    # Create a .env file with the provided environment variables
    with open('.env', 'w') as f:
        f.write(f"aws_access_key_id={aws_access_key_id}\n")
        f.write(f"aws_secret_access_key={aws_secret_access_key}\n")
        f.write(f"region=us-east-1\n")
        f.write(f"table_name={table_name}\n")
        
        # Optional Qualtrics survey paths
        if qualtrics_survey_1a_path is not None:
            f.write(f"qualtrics_survey_1a_path={qualtrics_survey_1a_path}\n")
        if qualtrics_survey_1b_path is not None:
            f.write(f"qualtrics_survey_1b_path={qualtrics_survey_1b_path}\n")
        if qualtrics_survey_2_path is not None:
            f.write(f"qualtrics_survey_2_path={qualtrics_survey_2_path}\n")
        if qualtrics_survey_3_path is not None:
            f.write(f"qualtrics_survey_3_path={qualtrics_survey_3_path}\n")
        if qualtrics_survey_4_path is not None:
            f.write(f"qualtrics_survey_4_path={qualtrics_survey_4_path}\n")
            
def check_incomplete_env_file():
    """See what required variables are missing from the .env file."""
    # Read the .env file directly to check only those variables
    env_vars = {}
    if os.path.exists('.env'):
        with open('.env', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
    
    missing_vars = []
    required_vars = ['aws_access_key_id', 'aws_secret_access_key', 'region', 'table_name']
    for var in required_vars:
        if var not in env_vars or not env_vars[var]:
            missing_vars.append(var)
    return missing_vars

def update_env_variable(variable: str, value: str):
    """Update a specific environment variable in the .env file."""
    # Load existing environment variables
    load_dotenv()
    
    # Write the updated variable to the .env file
    with open('.env', 'a') as f:
        f.write(f"\n{variable}={value}\n")
        
def get_env_variables() -> dict:
    """Get all environment variables from the .env file."""
    env_vars = {}
    
    load_dotenv()  # Load environment variables from .env file if it exists
    with open('.env', 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip()
    return env_vars

def update_or_create_env_var(env_vars: dict, variable: str, value: str):
    """Update an existing environment variable or create a new one in the .env file."""
    # Load existing environment variables
    load_dotenv()

    # Check if the variable already exists in the local env file
    if variable in env_vars:
        # Update the existing variable
        env_vars[variable] = value
    else:
        # Create a new variable
        env_vars[variable] = value

    # Write all environment variables back to the .env file
    with open('.env', 'w') as f:
        for key, value in env_vars.items():
            f.write(f"{key}={value}\n")