from flask import Flask, request, render_template
import pika, json 
from datetime import datetime

app = Flask(__name__)
QUEUE_NAME = 'transactions'

def send_to_rabbitmq(data):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME,durable=False)

    channel.basic_publish(
        exchange='',
        routing_key=QUEUE_NAME,
        body=json.dumps(data).encode('utf-8'),
        properties=pika.BasicProperties(delivery_mode=1) # non-persistent
    )
    print("[✔] Sent to queue:", QUEUE_NAME)
    connection.close()

@app.route('/', methods=['GET', 'POST'])
def index():
    success = False

    if request.method == 'POST':
        time = datetime.now()
        data = {
            "transaction_id": "TX" + time.strftime("%Y%m%d%H%M%S"),
            "customer_id": request.form['customer_id'],
            "amount": request.form['amount'],
            "merchant":request.form['merchant'],
            "location":request.form['location'],
            "payment_method": request.form['payment_method'],
            "device_id": request.form['device_id'],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            send_to_rabbitmq(data)
            success = True
        except:
            success = False
    
    return render_template('./index.html', success=success)


if __name__ == '__main__':
    app.run(debug=True)