import logging
import os

from dotenv import load_dotenv

load_dotenv()

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

BASE_URL = "alpha-vantage.p.rapidapi.com"

url = f"https://{BASE_URL}/query"

api_key = os.getenv("API_KEY")

headers = {
	"x-rapidapi-key": api_key,
	"x-rapidapi-host": BASE_URL
}
