from setuptools import setup, find_packages

setup(
    name="recruitment",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.68.0",
        "uvicorn>=0.15.0",
        "aio_pika>=10.0.0",
        "pydantic>=1.8.0",
        "python-dotenv>=0.19.0",
        "sqlalchemy>=1.4.0",
        "requests>=2.26.0",
        "beautifulsoup4>=4.9.0",
        "apscheduler>=3.9.0",
        "pika>=1.2.0",
        "googlesearch-python>=1.2.0",
    ],
    python_requires=">=3.8",
    author="Christo Strydom",
    author_email="christo.strydom@gmail.com",
    description="A recruitment data processing system",
    long_description=open("README.md").read() if open("README.md").read() else "",
    long_description_content_type="text/markdown",
    entry_points={
        "console_scripts": [
            "url-discovery=recruitment.url_discovery_service:main",
            "url-processing=recruitment.url_processing_service:main",
        ],
    },
) 