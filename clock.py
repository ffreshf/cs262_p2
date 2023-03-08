from multiprocessing import Process 
import os 
import socket 
from _thread import * 
import threading 
import time 
from threading import Thread 
import logging
import random
import csv

msg_queue = []
msg_lock = threading.Lock()

class Clock:
    def __init__(self):
        self.time = 0
    
    def getTime(self):
        return self.time

    # Update logical clock with respect to rules we learned in lecture
    def update(self, new):
        self.time = max(self.time, new) + 1 

    # Update per rule for non-message-receive events
    def increment(self): 
        self.time += 1

# thread always listening for incoming messages and appends them on the queue
def consumer(conn):
    global msg_queue
    global msg_lock

    # constantly listening
    while True:
        # Data is received
        data = conn.recv(1024)

        # If data is non-empty
        if data:
            # Message is decoded
            decoded_msg = str(data.decode('ascii'))

            # Message is appended to the queue
            msg_lock.acquire()
            msg_queue.append(decoded_msg)
            msg_lock.release()

def producer(source, consumerA, consumerB):
    host = "127.0.0.1"

    global msg_queue
    global msg_lock

    # setting up .csv configuration
    output = open("log_" + str(source) + ".csv", "w")
    writer = csv.writer(output)
    writer.writerow(["machine", "speed", "globaltime", "clocktime"])

    # number of clock ticks per (real world) second for this machine
    ticks = random.randint(1,6)
    # corresponding sleep value
    sleepVal = 1/ticks

    # each machine has a logical clock
    clock = Clock()

    # virtual machine connects to each of the other virtual machines so that messages can be passed between them
    machineA = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    machineB = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

    try:
        # connects to one of the other machines
        machineA.connect((host, source))
        print("Process on port " + str(consumerA) + " successfully connected to port val:" + str(source) + "\n")
        # connnects to the other machine
        machineB.connect((host, source))
        print("Process on port " + str(consumerB) + " successfully connected to port val:" + str(source) + "\n")
        
        # set up file for logging
        logging.basicConfig(filename="machine" + str(source) + ".log", filemode='w', format='%(levelname)s:%(message)s', level=logging.DEBUG)

        # title of log file
        logging.info("Machine at port: " + str(source) + ". Clock speed: " + str(ticks) + " ticks per second.")

        while True:
            writer.writerow([str(source), str(ticks), str(time.time()), str(clock.getTime())])

            time.sleep(sleepVal)

            # If there is a message in the message queue for the machine
            if len(msg_queue) > 0:
                # take one message off the queue
                msg_lock.acquire()
                msg = msg_queue.pop(0)
                msg_lock.release()

                # update the local logical clock with respect to message received
                clock.update(int(msg))

                # log that it received a message, the global time (gotten from the system), the length of the message queue, and the logical clock time.
                logging.info("Message received. Global time: " + str(time.time()) + ". Queue length: " + str(len(msg_queue)) + ". Clock time: " + str(clock.getTime()) + ".")

            # If there is no message in the queue
            else:
                # generate a random number in the range of 1-10
                code = random.randint(1,10)

                # if the value is 1
                if code == 1:
                    # send to one of the other machines a message that is the local logical clock time
                    machineA.sendto(str(clock.getTime()).encode('ascii'), (host,consumerA)) 

                    # update its own logical clock
                    clock.increment()

                    # update the log with the send, the system time, and the logical clock time
                    logging.info("Message sent to " + str(consumerA) + ". Global time: " + str(time.time()) + ". Clock time: " + str(clock.getTime()) + ".")

                # if the value is 2
                elif code == 2:
                    # send to the other virtual machine a message that is the local logical clock time
                    machineB.sendto(str(clock.getTime()).encode('ascii'), (host,consumerB))

                    # update its own logical clock
                    clock.increment()

                    # update the log with the send, the system time, and the logical clock time
                    logging.info("Message sent to " + str(consumerB) + ". Global time: " + str(time.time()) + ". Clock time: " + str(clock.getTime()) + ".")

                # if the value is 3,
                elif code == 3:
                    # send to both of the other virtual machines a message that is the logical clock time
                    machineA.sendto(str(clock.getTime()).encode('ascii'), (host,consumerA)) 
                    machineB.sendto(str(clock.getTime()).encode('ascii'), (host,consumerB))

                    # update its own logical clock
                    clock.increment()

                    # update the log with the send, the system time, and the logical clock time
                    logging.info("Message sent to " + str(consumerA) + ". Global time: " + str(time.time()) + ". Clock time: " + str(clock.getTime()) + ".")
                    logging.info("Message sent to " + str(consumerB) + ". Global time: " + str(time.time()) + ". Clock time: " + str(clock.getTime()) + ".")
                
                # if the value is other than 1-3
                else:
                    # treat the cycle as an internal event

                    # update the local logical clock
                    clock.increment()

                    # log the internal event, the system time, and the logical clock value
                    logging.info("Internal event. Global time: " + str(time.time()) + ". Clock time: " + str(clock.getTime()) + ".")

    except socket.error as e: print ("Error connecting producer: %s" % e)

# Method for initializing machine
def init_machine(config):
    HOST = str(config[0])
    PORT = int(config[1])
    print("starting server | port val:", PORT)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen()
    while True:
        conn, addr = s.accept()
        start_new_thread(consumer, (conn,))

# Method for each machine
def machine(config):
    config.append(os.getpid())

    init_thread = Thread(target=init_machine, args=(config,))
    init_thread.start()

    # add delay to initialize the server-side logic on all processes 
    time.sleep(5)
    
    # extensible to multiple producers 
    prod_thread = Thread(target=producer, args=(config[2], config[1], config[3]))
    prod_thread.start()

localHost = "127.0.0.1"
 
if __name__ == '__main__':
    port1 = 2056
    port2 = 3056
    port3 = 4056

    # format of config is localhost, consumer, producer, other machine (consumer)
    config1 = [localHost, port1, port2, port3]
    p1 = Process(target=machine, args=(config1,))
    config2 = [localHost, port2, port3, port1]
    p2 = Process(target=machine, args=(config2,))
    config3 = [localHost, port3, port1, port2]
    p3 = Process(target=machine, args=(config3,))

    # Start processes
    p1.start()
    p2.start()
    p3.start()

    # Join processes
    p1.join()
    p2.join()
    p3.join()
