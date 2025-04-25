## URL Processing Dashboard

The URL Processing Dashboard is a Streamlit application that allows you to monitor and manually process URLs from the recruitment database. It provides a step-by-step interface for processing job postings and debugging any issues that arise.

### Features

- View all URLs from the database with their current processing status
- Select individual URLs for processing
- Step through each processing stage manually:
  1. Web Crawling
  2. Company Information Processing
  3. Job Details Processing
  4. Skills Processing
  5. Location Processing
  6. Benefits Processing
  7. Contacts Processing
  8. Job Advert Processing
  9. Industry Processing
- View detailed error messages and processing results
- Monitor the progress of each processing step

### Installation

1. Install Streamlit if you haven't already:
```bash
pip install streamlit
```

### Usage

1. Start the Streamlit app:
```bash
streamlit run streamlit_app.py
```

2. The app will open in your default web browser. You can then:
   - Select a URL from the dropdown menu
   - Click the "Crawl Website" button to start the process
   - After crawling, click each subsequent button to process different aspects of the job posting
   - View the results and any errors that occur during processing

### Troubleshooting

If you encounter any issues:
- Check that the database is properly initialized and accessible
- Verify that all required Python packages are installed
- Check the Streamlit console for detailed error messages
- Ensure the web crawler has proper access to the target URLs
