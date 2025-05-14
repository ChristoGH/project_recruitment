import os
import sys

# Get the absolute path to the project root
project_root = os.path.dirname(os.path.abspath(__file__))

# Add the project root to the Python path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Project root: {project_root}")
print(f"Python path: {sys.path}")

try:
    # Try absolute import
    from recruitment.logging_config import setup_logging

    print("Successfully imported logging_config using absolute import")

    # Initialize logger
    logger = setup_logging("test")
    print("Successfully initialized logger")
except ImportError as e:
    print(f"ImportError: {e}")
    print(
        f"Recruitment directory exists: {os.path.exists(os.path.join(project_root, 'recruitment'))}"
    )
    print(
        f"Logging config exists: {os.path.exists(os.path.join(project_root, 'recruitment', 'logging_config.py'))}"
    )

    # List contents of recruitment directory
    print("\nContents of recruitment directory:")
    for item in os.listdir(os.path.join(project_root, "recruitment")):
        print(f"  - {item}")
