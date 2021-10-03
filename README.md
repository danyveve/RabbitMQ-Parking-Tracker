# RabbitMQ-Parking-Tracker

Requirement and solution for a RabbitMQ task/assignment.

## Task
The assignment is to build a smart parking application. The scenario comprises a Parking Sensor that detects when cars drive in and out of a parking lot. The application logic resides in the Parking Worker (backend) where entry and exit event messages generated by the parking sensor are processed to calculate a parking cost and to generate billing events. Customers that drive in and out of the parking lot receive alerts on their Customer App when they do so; they also receive billing information for parking when exiting the parking lot.

My task is to create the necessary RabbitMQ messaging exchanges and queues for such a scenario as specified in the Requirements below.

### Explicative diagram of the architecture

<img src="architecture.png" />

(note that the consistent hash exchange plugin must be enabled before running my solution)

### Testing the app

The scripts from the "scripts" folder can be used for testing the application.

1. Start a worker with python3 run_worker.py --id "<worker_id>" --queue "<worker_id>_queue" -w "1"
2. Start a customer app with python3 run_customer_app.py -c "<customer_id>"
3. Generate ParkingEvents through python3 produce_parking_event.py -e "entry" -c "<customer_id>" -t 0 and python3 produce_parking_event.py -e "exit" -c "<customer_id>" -t <num_minutes>
