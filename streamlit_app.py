import os
import sys
import logging
import logging.handlers
from pathlib import Path

# Get the absolute path to the project root
project_root = os.path.dirname(os.path.abspath(__file__))

# Add the project root to the Python path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import streamlit as st
import sqlite3
from typing import List, Dict, Any
import pandas as pd
from urllib.parse import urlparse
from recruitment.recruitment_db import RecruitmentDatabase
from recruitment.batch_processor import (
    process_company, process_job, process_skills, process_location,
    process_benefits, process_contacts, process_job_advert, process_industry
)
from libraries.web_crawler_lib import WebCrawlerResult, crawl_website
import asyncio
import json

def setup_logging(log_name="app", log_dir=None, log_level=None):
    """Configure application logging."""
    # Get log directory from environment or use default
    log_dir = log_dir or os.getenv('LOG_DIR', 'logs')
    
    # Get log level from environment or use default
    log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Create logs directory
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)

    # Create logger
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)

    # Clear existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers = []

    # Detailed formatter for debugging
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler
    file_handler = logging.FileHandler(
        log_path / f"{log_name}.log",
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

# Initialize logging
logger = setup_logging("streamlit_app")

# Initialize session state
if 'selected_url' not in st.session_state:
    st.session_state.selected_url = None
if 'crawler_result' not in st.session_state:
    st.session_state.crawler_result = None
if 'processing_steps' not in st.session_state:
    st.session_state.processing_steps = []

def get_urls_from_db() -> List[Dict[str, Any]]:
    """Get all URLs from the database."""
    db = RecruitmentDatabase()
    with db._get_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM urls ORDER BY id")
        return [dict(row) for row in cursor.fetchall()]

def get_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urlparse(url).netloc
    except:
        return ""

async def process_url_step(url_id: int, step_name: str, processor_func, data: Dict, perform_insert: bool = False):
    """Process a single step of URL processing."""
    try:
        db = RecruitmentDatabase()
        if perform_insert:
            processor_func(db, url_id, data)
            return True, f"Successfully processed and inserted {step_name}"
        else:
            # Just show what would be processed
            return True, f"Would process {step_name} with data: {json.dumps(data, indent=2)}"
    except Exception as e:
        return False, f"Error processing {step_name}: {str(e)}"

def main():
    st.title("URL Processing Dashboard")
    
    # Get URLs from database
    urls = get_urls_from_db()
    
    # Create a DataFrame for the URLs
    df = pd.DataFrame(urls)
    if not df.empty:
        df['domain'] = df['url'].apply(get_domain)
    
    # URL Selection
    st.header("Select URL to Process")
    if not df.empty:
        selected_index = st.selectbox(
            "Choose a URL",
            range(len(df)),
            format_func=lambda x: f"{df.iloc[x]['url']} ({df.iloc[x]['processing_status']})"
        )
        st.session_state.selected_url = df.iloc[selected_index]
        st.write(f"Selected URL: {st.session_state.selected_url['url']}")
    else:
        st.warning("No URLs found in the database")
        return
    
    # Processing Steps
    st.header("Processing Steps")
    
    # Step 1: Web Crawling
    if st.button("1. Crawl Website"):
        with st.spinner("Crawling website..."):
            try:
                result = asyncio.run(crawl_website(
                    url=st.session_state.selected_url['url'],
                    word_count_threshold=10,
                    excluded_tags=['form', 'header'],
                    exclude_external_links=True,
                    process_iframes=True,
                    remove_overlay_elements=True,
                    use_cache=True,
                    verbose=True
                ))
                st.session_state.crawler_result = result
                st.success("Website crawled successfully!")
                st.json(result.__dict__)
            except Exception as e:
                st.error(f"Error crawling website: {str(e)}")
    
    # Step 2: Process Company Information
    if st.session_state.crawler_result:
        st.subheader("2. Process Company Information")
        perform_insert = st.checkbox("Insert into database", key="company_insert")
        if st.button("Process Company Information"):
            with st.spinner("Processing company information..."):
                success, message = asyncio.run(process_url_step(
                    st.session_state.selected_url['id'],
                    "company information",
                    process_company,
                    st.session_state.crawler_result.__dict__,
                    perform_insert
                ))
                if success:
                    st.success(message)
                else:
                    st.error(message)
    
    # Step 3: Process Job Details
    if st.session_state.crawler_result:
        st.subheader("3. Process Job Details")
        perform_insert = st.checkbox("Insert into database", key="job_insert")
        if st.button("Process Job Details"):
            with st.spinner("Processing job details..."):
                success, message = asyncio.run(process_url_step(
                    st.session_state.selected_url['id'],
                    "job details",
                    process_job,
                    st.session_state.crawler_result.__dict__,
                    perform_insert
                ))
                if success:
                    st.success(message)
                else:
                    st.error(message)
    
    # Step 4: Process Skills
    if st.session_state.crawler_result:
        st.subheader("4. Process Skills")
        perform_insert = st.checkbox("Insert into database", key="skills_insert")
        if st.button("Process Skills"):
            with st.spinner("Processing skills..."):
                success, message = asyncio.run(process_url_step(
                    st.session_state.selected_url['id'],
                    "skills",
                    process_skills,
                    st.session_state.crawler_result.__dict__,
                    perform_insert
                ))
                if success:
                    st.success(message)
                else:
                    st.error(message)
    
    # Step 5: Process Location
    if st.session_state.crawler_result:
        st.subheader("5. Process Location")
        perform_insert = st.checkbox("Insert into database", key="location_insert")
        if st.button("Process Location"):
            with st.spinner("Processing location..."):
                success, message = asyncio.run(process_url_step(
                    st.session_state.selected_url['id'],
                    "location",
                    process_location,
                    st.session_state.crawler_result.__dict__,
                    perform_insert
                ))
                if success:
                    st.success(message)
                else:
                    st.error(message)
    
    # Step 6: Process Benefits
    if st.session_state.crawler_result:
        st.subheader("6. Process Benefits")
        perform_insert = st.checkbox("Insert into database", key="benefits_insert")
        if st.button("Process Benefits"):
            with st.spinner("Processing benefits..."):
                success, message = asyncio.run(process_url_step(
                    st.session_state.selected_url['id'],
                    "benefits",
                    process_benefits,
                    st.session_state.crawler_result.__dict__,
                    perform_insert
                ))
                if success:
                    st.success(message)
                else:
                    st.error(message)
    
    # Step 7: Process Contacts
    if st.session_state.crawler_result:
        st.subheader("7. Process Contacts")
        perform_insert = st.checkbox("Insert into database", key="contacts_insert")
        if st.button("Process Contacts"):
            with st.spinner("Processing contacts..."):
                success, message = asyncio.run(process_url_step(
                    st.session_state.selected_url['id'],
                    "contacts",
                    process_contacts,
                    st.session_state.crawler_result.__dict__,
                    perform_insert
                ))
                if success:
                    st.success(message)
                else:
                    st.error(message)
    
    # Step 8: Process Job Advert
    if st.session_state.crawler_result:
        st.subheader("8. Process Job Advert")
        perform_insert = st.checkbox("Insert into database", key="job_advert_insert")
        if st.button("Process Job Advert"):
            with st.spinner("Processing job advert..."):
                success, message = asyncio.run(process_url_step(
                    st.session_state.selected_url['id'],
                    "job advert",
                    process_job_advert,
                    st.session_state.crawler_result.__dict__,
                    perform_insert
                ))
                if success:
                    st.success(message)
                else:
                    st.error(message)
    
    # Step 9: Process Industry
    if st.session_state.crawler_result:
        st.subheader("9. Process Industry")
        perform_insert = st.checkbox("Insert into database", key="industry_insert")
        if st.button("Process Industry"):
            with st.spinner("Processing industry..."):
                success, message = asyncio.run(process_url_step(
                    st.session_state.selected_url['id'],
                    "industry",
                    process_industry,
                    st.session_state.crawler_result.__dict__,
                    perform_insert
                ))
                if success:
                    st.success(message)
                else:
                    st.error(message)

if __name__ == "__main__":
    main() 