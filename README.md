# LinkedIn Scraper

A Python-based LinkedIn scraper that continuously monitors and extracts data from LinkedIn profiles and company pages.

## Features

- Automated LinkedIn profile and company page scraping
- Continuous monitoring capabilities
- Data extraction and processing
- Streamlit-based user interface
- Docker support

## Requirements

- Python 3.7+
- Chrome/Chromium browser
- Required Python packages listed in `requirements.txt`

## Installation

1. Clone the repository:
```bash
git clone <your-repo-url>
cd <repo-name>
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file with the following variables:
```
LINKEDIN_USERNAME=your_username
LINKEDIN_PASSWORD=your_password
```

## Usage

1. Run the Streamlit app:
```bash
streamlit run app.py
```

2. Or run the scraper directly:
```bash
python continuous_linkedin_scraper.py
```

## Docker Support

Build and run using Docker:

```bash
docker build -t linkedin-scraper .
docker run -p 8501:8501 linkedin-scraper
```

## Security Note

Make sure to:
1. Never commit sensitive credentials
2. Keep your `.env` file secure
3. Regularly rotate any API keys or tokens

## License

[Your chosen license] 