from dotenv import load_dotenv
import os

# Load variables from .env into the environment
load_dotenv()

# Access the variables
CLOUD_ID = os.getenv("CLOUD_ID")
ADMIN = os.getenv("ADMIN")
PASSWORD = os.getenv("PASSWORD")
