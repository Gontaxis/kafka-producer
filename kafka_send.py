from confluent_kafka import Producer
import json
from flask import Flask, request, jsonify

# Kafka configuration
conf = {
    'bootstrap.servers': 'broker-1-yy9cpm55h68h5w83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093,broker-0-yy9cpm55h68h5w83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093,broker-4-yy9cpm55h68h5w83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093,broker-3-yy9cpm55h68h5w83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093,broker-2-yy9cpm55h68h5w83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093,broker-5-yy9cpm55h68h5w83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'token',
    'sasl.password': 'R3nItV_R7h7ID4nsOEYhTbmtg-BHWr40o41_HfWQmoF6',
    'client.id': 'python-producer'
}

# Create Producer instance
producer = Producer(**conf)

# Callback to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Create Flask app
app = Flask(__name__)

@app.route('/produce', methods=['POST'])
def produce_message():
    # Get the JSON payload from the request
    message_payload = request.json
    topic = 'viaje'  # Replace with your topic name

    try:
        # Produce message to Kafka
        # Use 'id_chofer' as the key for partitioning and tracking messages
        producer.produce(topic, key=str(message_payload['id_chofer']), value=json.dumps(message_payload), callback=delivery_report)
        producer.flush()
        return jsonify({"status": "Message sent to Kafka"}), 200
    except Exception as e:
        return jsonify({"status": "Error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5002, debug=True)

