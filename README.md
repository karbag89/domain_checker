# domain_checker
Checking domain information (such as domain name, expiration date, creation date, country, city, zip code, state, address), writing value to PostgreSQL table using RabbitMQ message broker.

On this project you need to use Python 3.7.5 or higher version program.

Please install necessary Python libraries using requirements.txt file using "pip3 install -r requirements.txt" command.

Before running main Python files "generate_domains.py" and "worker.py" you need to set appropriate credentials in "generate_domains.py" and "worker.py" Python files then run it.

To speed up the "generate_domains.py" file I was used Python threads.

You can run multiple worker(s) (parallel) at the same time.

Note: All Python files checked on flake8 toolkit (flake8 is a toolkit for checking your code base against coding style (PEP8), programming errors (like “library imported but unused” and “Undefined name”) and to check cyclomatic complexity).
