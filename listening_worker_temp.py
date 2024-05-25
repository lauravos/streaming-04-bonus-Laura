"""
Laura Gagnon-Vos
05/24/2024

    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Author: Laura Gagnon-Vos
    Date: May 24, 2024

"""

import pika
import sys
import time
import csv

#import and setup logging
from util_logger import setup_logger 
logger, logname = setup_logger(__file__)

#function to convert fahrenheit to celsius
def convert_c(f):
    f = (int(f)-32)*5/9
    return round(f,2)

# define a callback function to be called when a message is received
def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # takes the received message and coverts to celsius
    celsiusTemp = convert_c(body.decode())

#create CSV and write to it
    csvwriter = csv.writer(open("data_tempOutputfile.csv", "a"))
    #csvwriter = open('cityOutputfile.csv', 'a')
    originalString = str(body.decode())
    csvwriter.writerow(zip([originalString], [celsiusTemp]))


    logger.info(f" [x] Received {celsiusTemp}")
    # simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # when done with task, tell the user
    #print(" [x] Done.")
    logger.info(" [x] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main function to run the program
def main(hn: str = "localhost", qn: str = "task_temp"):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        #print()
        #print("ERROR: connection to RabbitMQ server failed.")
        #print(f"Verify the server is running on host={hn}.")
        #print(f"The error says: {e}")
        #print()
        #sys.exit(1)

        logger.info()
        logger.info("ERROR: connection to RabbitMQ server failed.")
        logger.info(f"Verify the server is running on host={hn}.")
        logger.info(f"The error says: {e}")
        logger.info()
        sys.exit(1)



    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume( queue=qn, on_message_callback=callback)

        # print a message to the console for the user
        #print(" [*] Ready for work. To exit press CTRL+C")
        logger.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        #print()
        #print("ERROR: something went wrong.")
       # print(f"The error says: {e}")
       # sys.exit(1)
        logger.info()
        logger.info("ERROR: something went wrong.")
        logger.info(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        #print()
        #print(" User interrupted continuous listening process.")
        #sys.exit(0)
        logger.info()
        logger.info(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        #print("\nClosing connection. Goodbye.\n")
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost", "task_temp")
