import os
import sys
import logging
import logging.handlers
from pathlib import Path
from typing import List, Dict, Any, Optional
import pika
import json

# Get the absolute path to the project root
project_root = os.path.dirname(os.path.abspath(__file__))

# Add the project root to the Python path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import streamlit as st
import pandas as pd
from urllib.parse import urlparse
from libraries.web_crawler_lib import WebCrawlerResult, crawl_website
import asyncio
from recruitment.prompts import LIST_PROMPTS, NON_LIST_PROMPTS, COMPLEX_PROMPTS
from recruitment.utils import get_model_for_prompt

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

def get_rabbitmq_connection():
    """Get a connection to RabbitMQ."""
    try:
        rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
        rabbitmq_port = int(os.getenv("RABBITMQ_PORT", "5672"))
        rabbitmq_user = os.getenv("RABBITMQ_USER", "guest")
        rabbitmq_password = os.getenv("RABBITMQ_PASSWORD", "guest")
        
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        parameters = pika.ConnectionParameters(
            host=rabbitmq_host,
            port=rabbitmq_port,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        
        connection = pika.BlockingConnection(parameters)
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        return None

def get_urls_from_queue() -> List[Dict[str, Any]]:
    """Get URLs from RabbitMQ queue."""
    try:
        connection = get_rabbitmq_connection()
        if not connection:
            return []
            
        channel = connection.channel()
        queue_name = "recruitment_urls"
        channel.queue_declare(queue=queue_name, durable=True)
        
        # Get message count
        queue_info = channel.queue_declare(queue=queue_name, durable=True, passive=True)
        message_count = queue_info.method.message_count
        
        if message_count == 0:
            return []
            
        # Get messages without consuming them
        urls = []
        for _ in range(min(message_count, 100)):  # Limit to 100 messages
            method, properties, body = channel.basic_get(queue=queue_name)
            if method:
                message = json.loads(body)
                urls.append({
                    "url": message.get("url"),
                    "search_id": message.get("search_id"),
                    "timestamp": message.get("timestamp"),
                    "processing_status": "pending"  # Since it's in the queue, it's pending
                })
                # Requeue the message
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        connection.close()
        return urls
    except Exception as e:
        logger.error(f"Error getting URLs from queue: {e}")
        return []

def get_domain(url: str) -> str:
    """Extract domain from URL."""
    try:
        return urlparse(url).netloc
    except:
        return ""

async def process_prompt(prompt_type: str, text: str) -> Dict[str, Any]:
    """Process a prompt using the chat engine."""
    try:
        # Get the appropriate prompt
        if prompt_type in LIST_PROMPTS:
            prompt = LIST_PROMPTS[prompt_type]
        elif prompt_type in NON_LIST_PROMPTS:
            prompt = NON_LIST_PROMPTS[prompt_type]
        elif prompt_type in COMPLEX_PROMPTS:
            prompt = COMPLEX_PROMPTS[prompt_type]
        else:
            return {"error": f"Unknown prompt type: {prompt_type}"}

        # Here you would typically call your LLM with the prompt and text
        # For this example, we'll simulate a response with the correct format
        if prompt_type == "recruitment_prompt":
            response = {
                "answer": "yes",
                "evidence": ["The page contains job requirements", "There is a salary range mentioned"]
            }
        elif prompt_type == "company_prompt":
            response = {"company": "Example Company"}
        elif prompt_type == "job_prompt":
            response = {"jobs": ["Software Engineer", "Data Scientist"]}  # Changed to use jobs list
        elif prompt_type == "benefits_prompt":
            response = {"benefits": ["Health insurance", "401k", "Remote work"]}
        elif prompt_type == "skills_prompt":
            response = {"skills": ["Python", "SQL", "Machine Learning"]}
        elif prompt_type == "duties_prompt":
            response = {"duties": ["Develop software", "Write documentation", "Code review"]}
        elif prompt_type == "qualifications_prompt":
            response = {"qualifications": ["Bachelor's degree", "5 years experience"]}
        else:
            response = {"error": "No response generated"}

        # Validate the response using the appropriate model
        try:
            model = get_model_for_prompt(prompt_type)
            validated_response = model(**response)
            return validated_response.model_dump()
        except Exception as e:
            logger.error(f"Error validating response: {e}")
            return {"error": f"Validation error: {str(e)}"}

    except Exception as e:
        logger.error(f"Error processing prompt: {e}")
        return {"error": f"Processing error: {str(e)}"}

def main():
    st.title("URL Processing Dashboard")
    
    # Get URLs from RabbitMQ queue
    urls = get_urls_from_queue()
    
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
        st.warning("No URLs found in the queue")
        return
    
    # Web Crawling
    if st.button("Extract Text from Website"):
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
                
                # Display extracted text
                st.subheader("Extracted Text")
                st.text_area("Website Content", result.markdown, height=300)
            except Exception as e:
                st.error(f"Error crawling website: {str(e)}")
    
    # Process with Chat Engine
    if st.session_state.crawler_result:
        st.header("Process with Chat Engine")
        
        # First, get jobs from the job_prompt
        if "jobs" not in st.session_state:
            with st.spinner("Extracting jobs..."):
                try:
                    result = asyncio.run(process_prompt(
                        "job_prompt",
                        st.session_state.crawler_result.markdown
                    ))
                    if "jobs" in result:
                        st.session_state.jobs = result["jobs"]
                        st.success(f"Found {len(result['jobs'])} jobs!")
                    else:
                        st.error("No jobs found in the response")
                except Exception as e:
                    st.error(f"Error extracting jobs: {str(e)}")
        
        # If we have jobs, let the user select which ones to process
        if "jobs" in st.session_state and st.session_state.jobs:
            st.subheader("Select Jobs to Process")
            selected_jobs = st.multiselect(
                "Choose jobs to process",
                st.session_state.jobs,
                default=st.session_state.jobs  # Select all by default
            )
            
            if selected_jobs:
                st.subheader("Select Prompt Type")
                prompt_types = list(LIST_PROMPTS.keys()) + list(NON_LIST_PROMPTS.keys()) + list(COMPLEX_PROMPTS.keys())
                selected_prompt = st.selectbox("Choose a prompt type", prompt_types)
                
                if st.button("Process Selected Jobs"):
                    with st.spinner("Processing..."):
                        try:
                            # Process each selected job
                            for job in selected_jobs:
                                st.write(f"Processing {job}...")
                                # Add job context to the prompt
                                text = f"Job Title: {job}\n\n{st.session_state.crawler_result.markdown}"
                                result = asyncio.run(process_prompt(
                                    selected_prompt,
                                    text
                                ))
                                st.json(result)
                        except Exception as e:
                            st.error(f"Error processing jobs: {str(e)}")

if __name__ == "__main__":
    main() 