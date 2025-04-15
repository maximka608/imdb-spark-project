# imdb-spark-project

### Set up

### Installing Dependencies
Navigate to the root directory of your project and install the necessary packages:

```bash
pip install -r requirements.txt
```

### Setting the Dataset Path
```yaml
services:
  my-spark:
    build: .
    volumes:
      - /path/to/your/local/dataset:/utils/data
    working_dir: /utils
    stdin_open: true
    tty: true
```
In docker-compose.yml replace /path/to/your/local/dataset with the absolute path to your dataset directory on your machine.

### Building the Docker Container
```bash
docker-compose build
```

### Launching the Service
```bash
docker-compose up
```