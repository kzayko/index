#!/usr/bin/env python3
"""Script to create Elasticsearch users and roles."""
import os
import sys
import time
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Load secrets.env if it exists (override with environment variables)
secrets_path = os.path.join(os.path.dirname(__file__), 'secrets.env')
if os.path.exists(secrets_path):
    load_dotenv(secrets_path, override=False)
# Also try /app/secrets.env for Docker
docker_secrets_path = '/app/secrets.env'
if os.path.exists(docker_secrets_path):
    load_dotenv(docker_secrets_path, override=False)

ELASTIC_PASSWORD = os.getenv('ELASTIC_PASSWORD', 'elastic_password_123')
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST', 'http://localhost:9200')
APP_USERNAME = os.getenv('ELASTICSEARCH_APP_USERNAME', 'app_user')
APP_PASSWORD = os.getenv('ELASTICSEARCH_APP_PASSWORD', 'app_password_123')


def wait_for_elasticsearch():
    """Wait for Elasticsearch to be ready."""
    print("Waiting for Elasticsearch to be ready...")
    max_retries = 30
    for i in range(max_retries):
        try:
            response = requests.get(
                f"{ELASTICSEARCH_HOST}/_cluster/health",
                auth=HTTPBasicAuth('elastic', ELASTIC_PASSWORD),
                timeout=5
            )
            if response.status_code == 200:
                print("Elasticsearch is ready!")
                return True
        except Exception as e:
            if i < max_retries - 1:
                print(f"Waiting for Elasticsearch... ({i+1}/{max_retries})")
                time.sleep(5)
            else:
                print(f"Failed to connect to Elasticsearch: {e}")
                return False
    return False


def create_app_user():
    """Create application user in Elasticsearch."""
    print(f"Creating application user: {APP_USERNAME}")
    
    url = f"{ELASTICSEARCH_HOST}/_security/user/{APP_USERNAME}"
    data = {
        "password": APP_PASSWORD,
        "roles": ["superuser"],
        "full_name": "Application User",
        "email": "app@example.com"
    }
    
    try:
        response = requests.post(
            url,
            auth=HTTPBasicAuth('elastic', ELASTIC_PASSWORD),
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            print(f"Application user '{APP_USERNAME}' created successfully!")
            return True
        elif response.status_code == 409:
            print(f"User '{APP_USERNAME}' already exists. Updating password...")
            # Try to update password
            update_url = f"{ELASTICSEARCH_HOST}/_security/user/{APP_USERNAME}/_password"
            update_response = requests.post(
                update_url,
                auth=HTTPBasicAuth('elastic', ELASTIC_PASSWORD),
                json={"password": APP_PASSWORD},
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            if update_response.status_code in [200, 204]:
                print(f"Password updated for user '{APP_USERNAME}'")
                return True
            else:
                print(f"Failed to update password: {update_response.text}")
                return False
        else:
            print(f"Failed to create user: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Error creating user: {e}")
        return False


def main():
    """Main function."""
    if not wait_for_elasticsearch():
        sys.exit(1)
    
    if create_app_user():
        print("\n" + "="*50)
        print("Elasticsearch user configuration:")
        print(f"  Username: {APP_USERNAME}")
        print(f"  Password: {APP_PASSWORD}")
        print("="*50)
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
