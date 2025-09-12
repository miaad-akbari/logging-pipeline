# MySQL to PostgreSQL Logging 

This project sets up a real-time logging pipeline that captures logs from a MySQL database, forwards them through a Kafka message broker using rsyslog, and finally stores them in a PostgreSQL database for analysis.

The entire environment is containerized with Docker Compose, and its deployment is automated with Ansible.

## Architecture

The data flows through the following components:

**MySQL** -> **Rsyslog** -> **Kafka** -> **Python Consumer** -> **PostgreSQL**

## Key Features

* **Fully Containerized**: All services run in isolated Docker containers for consistency and easy setup.
* **Scalable**: Built around Kafka, which allows the pipeline to handle high volumes of logs.
* **Automated Deployment**: Includes an Ansible playbook to deploy the entire stack on a fresh server with a single command.
* **Resilient**: The Python consumer includes error handling and connection retries to handle temporary service unavailability.


---

## Getting Started

You can run this project either manually using Docker Compose or automatically with the Ansible playbook.

### 1. Manual Setup

#### Prerequisites

* Docker
* Docker Compose

#### Steps

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/miaad-akbari/logging-pipeline.git](https://github.com/miaad-akbari/logging-pipeline.git)
    cd logging-pipeline
    ```

2.  **Build and start all services:**
    This command will download the necessary images, build the consumer app, and start all containers in the background.
    ```bash
    docker-compose up -d --build
    ```

3.  **Create the Kafka topic:**
    You only need to run this once.
    ```bash
    docker-compose exec kafka kafka-topics --create --topic mysql_logs --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
    ```

4.  **Generate some logs in MySQL:**
    Connect to the MySQL container and run some queries to generate logs.
    ```bash
    docker-compose exec mysql mysql -uroot -prootpassword -e "CREATE DATABASE IF NOT EXISTS testdb; USE testdb; CREATE TABLE IF NOT EXISTS test (id INT); INSERT INTO test VALUES (1);"
    ```

5.  **Check the logs in PostgreSQL:**
    Connect to the PostgreSQL container and check if the logs are being stored correctly.
    ```bash
    docker-compose exec postgres psql -U user -d mysqllogs -c "SELECT * FROM logs ORDER BY created_at DESC LIMIT 10;"
    ```

---

### 2. Automated Deployment with Ansible

#### Prerequisites

* Ansible installed on your local machine.
* A target server with SSH access.

#### Steps

1.  **Update the inventory:**
    Open the `ansible/inventories/servers/hosts` file and replace the placeholder with your server's IP and username.

2.  **Run the playbook:**
    Navigate to the `ansible` directory and run the following command:
    ```bash
    cd ansible
    ansible-playbook -i inventories/servers/hosts playbooks/deploy.yml
    ```
    This will install all dependencies, clone the project, and start the services on your target server.

---

## How It Works: A Visual Walkthrough

Here are a few screenshots showing the pipeline in action.

#### 1. Generating Logs in MySQL

First, I connected to the MySQL container to check if logging was enabled. As you can see, `general_log` is `ON` and writing to the correct file. I then ran a few SQL commands to create and modify a table.

![MySQL Log Configuration and Queries]
![alt text](<image/Screenshot From 2025-09-11 19-54-48.png>)


The `tail` command on the log file confirms that our SQL queries are being captured correctly.

![Viewing the MySQL Log File]
![alt text](<image/Screenshot From 2025-09-11 19-54-41.png>)



#### 2. Messages Arriving in Kafka

Next, I used `kafka-console-consumer` to listen to the `mysql_logs` topic. This screenshot shows that the log entries forwarded by rsyslog are arriving in Kafka as expected. (The "Hello Miaad" message was a simple test I sent with `kafka-console-producer` to confirm the topic was working).

![Kafka Topic Receiving Logs]
![alt text](<image/Screenshot From 2025-09-12 20-28-07.png>)

After this stage, the Python consumer picks up these messages and writes them to the PostgreSQL database, completing the pipeline.

---

## CI/CD Ideas

While this project doesn't have a full CI/CD pipeline, setting one up would be a great next step. A simple pipeline using **GitHub Actions** could look like this:

1.  **On Push to `main` branch:**
    * **Lint & Test:** Run a linter (like `pylint` or `flake8`) on the Python consumer code.
    * **Build & Scan Image:** Build the consumer's Docker image and scan it for vulnerabilities using a tool like `Trivy`.
2.  **On Tag (e.g., `v1.0.0`):**
    * **Push Image:** Push the tagged Docker image to a registry (like Docker Hub or GitHub Container Registry).
    * **Trigger Deployment:** Use a webhook or an Ansible Tower/AWX job to automatically run the Ansible playbook on the production server, which would pull the new image and restart the consumer service.