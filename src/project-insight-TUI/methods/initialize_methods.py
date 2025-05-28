import os
from dotenv import load_dotenv

# Function to see if there is a .env file in the current directory
def check_env_file_exists() -> bool:
    load_dotenv()  # Load environment variables from .env file if it exists
    return os.path.exists('.env')  # Check if .env file exists in the current directory

def check_env_variables() -> bool:
    """
    Check if the required environment variables are set.
    Returns True if all required variables are set, otherwise False.
    """
    required_vars = ['aws_access_key_id', 'aws_secret_access_key', 'region', 'table_name']
    for var in required_vars:
        if not os.getenv(var):
            print(f"Environment variable '{var}' is not set.")
            return False
    return True  # All required environment variables are set