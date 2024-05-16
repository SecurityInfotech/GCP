import os
import json
from flask import Flask, request
from google.cloud import storage, secretmanager, pubsub_v1

app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_file():
    message = request.get_json()
    if 'message' in message and 'data' in message['message']:
        file_data = json.loads(base64.b64decode(message['message']['data']).decode('utf-8'))
        file_name = file_data['name']
        bucket_name = file_data['bucket']

        # Initialize clients
        storage_client = storage.Client()
        secret_client = secretmanager.SecretManagerServiceClient()
        publisher = pubsub_v1.PublisherClient()

        # Get file metadata
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        metadata = blob.metadata or {}

        # Save metadata as secrets
        secret_id = f"{file_name.replace('/', '_')}_metadata"
        parent = f"projects/{os.getenv('GOOGLE_CLOUD_PROJECT')}/secrets/{secret_id}"

        # Create the secret
        try:
            secret_client.create_secret(
                request={"parent": parent, "secret_id": secret_id, "secret": {"replication": {"automatic": {}}}}
            )
        except Exception as e:
            # Secret already exists
            print(f"Secret {secret_id} already exists: {e}")

        # Add secret version
        payload = json.dumps(metadata).encode("UTF-8")
        secret_client.add_secret_version(
            request={"parent": parent, "payload": {"data": payload}}
        )

        # Delete the file from bucket
        blob.delete()

        # Publish message to Pub/Sub
        topic_name = os.getenv('TOPIC_NAME')
        topic_path = publisher.topic_path(os.getenv('GOOGLE_CLOUD_PROJECT'), topic_name)
        message_data = f"File {file_name} processed and secret created".encode("utf-8")
        future = publisher.publish(topic_path, data=message_data)
        future.result()

        return "File processed", 200
    else:
        return "Invalid message format", 400

if __name__ == "__main__":
    app.run(debug=True)
