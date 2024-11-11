import os
import time
import signal
import sys
import logging
import docker
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("db-manager")


class DatabaseError(Exception):
    """Custom exception for database-related errors"""

    pass


class DatabaseManager:
    def __init__(self):
        self._initialize_docker_client()
        self._load_configuration()
        self.shutdown = False

        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        # Wait for PostgreSQL to be ready
        self.wait_for_postgres()

    def _initialize_docker_client(self):
        """Initialize Docker client with proper error handling"""
        try:
            self.docker_client = docker.from_env()
            # Verify connectivity immediately
            self.docker_client.ping()
        except docker.errors.DockerException as e:
            logger.critical(
                "Failed to connect to Docker daemon. Is the Docker socket mounted?"
            )
            logger.critical(f"Error details: {str(e)}")
            logger.critical(
                "Please ensure you're running with: -v /var/run/docker.sock:/var/run/docker.sock:ro"
            )
            sys.exit(1)
        except Exception as e:
            logger.critical(f"Unexpected error connecting to Docker: {str(e)}")
            sys.exit(1)

    def _load_configuration(self):
        """Load configuration from environment variables with validation"""
        self.pg_host = os.environ.get("POSTGRES_HOST")
        if not self.pg_host:
            raise ValueError("POSTGRES_HOST environment variable is required")

        self.pg_user = os.environ.get("POSTGRES_USER")
        if not self.pg_user:
            raise ValueError("POSTGRES_USER environment variable is required")

        self.pg_password = os.environ.get("POSTGRES_PASSWORD")
        if not self.pg_password:
            raise ValueError("POSTGRES_PASSWORD environment variable is required")

        self.pg_port = int(os.environ.get("POSTGRES_PORT", "5432"))
        self.max_retries = int(os.environ.get("MAX_RETRIES", "30"))
        self.retry_interval = int(os.environ.get("RETRY_INTERVAL", "2"))

        # print configs
        logger.info(f"POSTGRES_HOST: {self.pg_host}")
        logger.info(f"POSTGRES_USER: {self.pg_user}")
        logger.info(f"POSTGRES_PORT: {self.pg_port}")
        logger.info(f"MAX_RETRIES: {self.max_retries}")
        logger.info(f"RETRY_INTERVAL: {self.retry_interval}")

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("Received shutdown signal, cleaning up...")
        self.shutdown = True

    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                user=self.pg_user,
                password=self.pg_password,
                connect_timeout=3,
            )
            conn.autocommit = True
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            raise DatabaseError(f"Failed to connect to database: {e}")
        finally:
            if conn:
                conn.close()

    def wait_for_postgres(self):
        """Wait for PostgreSQL to become available"""
        retry_count = 0
        while retry_count < self.max_retries and not self.shutdown:
            try:
                with self.get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        logger.info("Successfully connected to PostgreSQL")
                        return True
            except (DatabaseError, psycopg2.Error) as e:
                retry_count += 1
                logger.warning(
                    f"Waiting for PostgreSQL (attempt {retry_count}/{self.max_retries}): {e}"
                )
                time.sleep(self.retry_interval)

        if retry_count >= self.max_retries:
            raise RuntimeError("Failed to connect to PostgreSQL after maximum retries")
        return False

    def setup_database(self, db_name: str, db_user: str, password: str):
        """Set up database and user with proper error handling"""
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Validate inputs
                    if not all([db_name, db_user, password]):
                        raise ValueError(
                            "Database name, user, and password are required"
                        )

                    # Check if database exists
                    cur.execute(
                        "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
                    )
                    db_exists = cur.fetchone()

                    if not db_exists:
                        logger.info(f"Creating database: {db_name}")
                        cur.execute(
                            sql.SQL("CREATE DATABASE {}").format(
                                sql.Identifier(db_name)
                            )
                        )

                    # Check if user exists
                    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (db_user,))
                    user_exists = cur.fetchone()

                    if not user_exists:
                        logger.info(f"Creating user: {db_user}")
                        cur.execute(
                            sql.SQL("CREATE USER {} WITH PASSWORD %s").format(
                                sql.Identifier(db_user)
                            ),
                            (password,),
                        )

                    # Grant privileges
                    logger.info(f"Ensuring privileges for {db_user} on {db_name}")
                    cur.execute(
                        sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {}").format(
                            sql.Identifier(db_name), sql.Identifier(db_user)
                        )
                    )
        except (psycopg2.Error, DatabaseError) as e:
            logger.error(f"Failed to setup database {db_name}: {e}")
            raise DatabaseError(f"Database setup failed: {e}")

    def handle_container(self, container: docker.models.containers.Container):
        """Handle container database setup with validation"""
        try:
            labels = container.labels
            if labels.get("db.enable") != "true":
                return

            db_name = labels.get("db.name")
            db_user = labels.get("db.user")

            if not all([db_name, db_user]):
                logger.error(
                    f"Container {container.name} missing required labels, `db.name` or `db.user`"
                )
                return

            # Validate database name and user
            if not all(c.isalnum() or c == "_" for c in db_name):
                raise ValueError(f"Invalid database name: {db_name}")
            if not all(c.isalnum() or c == "_" for c in db_user):
                raise ValueError(f"Invalid database user: {db_user}")

            password_var = f"{db_user.upper()}_DB_PASSWORD"
            password = os.environ.get(password_var, "defaultpass")

            logger.info(f"Setting up database for container: {container.name}")
            self.setup_database(db_name, db_user, password)

        except (docker.errors.NotFound, docker.errors.APIError) as e:
            logger.error(f"Docker API error while handling container: {e}")
        except ValueError as e:
            logger.error(f"Validation error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error handling container {container.name}: {e}")

    def process_existing_containers(self):
        """Process existing containers with error handling"""
        try:
            containers = self.docker_client.containers.list()
            logger.info(f"Processing {len(containers)} existing containers")
            for container in containers:
                if not self.shutdown:
                    self.handle_container(container)
        except docker.errors.APIError as e:
            logger.error(f"Failed to list containers: {e}")
            raise RuntimeError(f"Docker API error: {e}")

    def watch_events(self):
        """Watch Docker events with error handling and graceful shutdown"""
        logger.info("Starting to watch for Docker events...")
        try:
            for event in self.docker_client.events(decode=True):
                if self.shutdown:
                    break

                if event["Type"] == "container" and event["Action"] == "start":
                    try:
                        container = self.docker_client.containers.get(event["id"])
                        self.handle_container(container)
                    except docker.errors.NotFound:
                        logger.warning(f"Container {event['id']} not found")
                    except Exception as e:
                        logger.error(f"Error handling container event: {e}")
        except docker.errors.APIError as e:
            logger.error(f"Docker events API error: {e}")
            raise RuntimeError(f"Docker events API error: {e}")

    def run(self):
        """Main run loop with error handling"""
        try:
            logger.info("Database manager starting...")
            self.process_existing_containers()

            while not self.shutdown:
                try:
                    self.watch_events()
                except Exception as e:
                    if not self.shutdown:
                        logger.error(f"Error in event watch loop: {e}")
                        time.sleep(5)  # Prevent rapid reconnection attempts

        except Exception as e:
            logger.critical(f"Fatal error: {e}")
            sys.exit(1)
        finally:
            logger.info("Database manager shutting down...")


def main():
    try:
        manager = DatabaseManager()
        manager.run()
    except Exception as e:
        logger.critical(f"Failed to start database manager: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
