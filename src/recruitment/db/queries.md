## Reduce the project.

Assistant, please look  the container setup  as defined in the docker-compose.yml file and Dockerfile.discovery, Dockerfile.processing, url_discovery_service and url_processing_service.py files.  I want to temporarily rework the wokflow to exclude any processing OTHER than storing urls in the db WHILE also including the scraped content.  

Please provide a detailed plan to achieve this in a manner whereby returning to the full flow inolves the least number of steps and disruption.  Following KISS to the letter while producing a worldclass solution.

Once the containeer is brought up I want the discovery service to find 100 urls every 60 mins and have the processing service start consuming those 30 mins later. 