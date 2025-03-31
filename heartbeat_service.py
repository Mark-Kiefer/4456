# Coded by Mark Kiefer (251237385)
# CS 4459B - Assignment 2
# Due by March 11, 2025

import grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
from google.protobuf import empty_pb2
from concurrent import futures
from datetime import datetime, timedelta
import time
import threading

# constant
SERVER_DOWN_THRESHOLD = 15

# global variables for recording last known time
server1_time = ""
server2_time = ""
server3_time = ""
server4_time = ""
server1_up = False
server2_up = False
server3_up = False
server4_up = False

class ViewServiceServicer(heartbeat_service_pb2_grpc.ViewServiceServicer):

    # function receives heartbeats from primary and backup and logs them
    def Heartbeat(self, request, context):

        global server1_time, server2_time, server3_time, server4_time, server1_up, server2_up, server3_up, server4_up

        identifier = request.service_identifier[0].upper() + request.service_identifier[1:]
        time = datetime.now()

        # update last known times
        if identifier == "Server_1":
            server1_time = time
            server1_up = True

        if identifier == "Server_2":
            server2_time = time
            server2_up = True

        if identifier == "Server_3":
            server3_time = time
            server3_up = True

        if identifier == "Server_4":
            server4_time = time
            server4_up = True

        # update log
        with open("heartbeat.txt", 'a') as file:
            file.write(f"{identifier} is alive. Latest heartbeat received at {time}\n")

        return empty_pb2.Empty() 


# repeatly checks on servers
def check_servers():

    global server1_time, server2_time, server3_time, server4_time, server1_up, server2_up, server3_up, server4_up

    while True:

        # server 1
        if server1_up:

            # time elasped greater than threshold
            if datetime.now() - server1_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Server 1 might be down. Latest heartbeat received at {server1_time}\n")
                
                server1_up = False

        # server 2
        if server2_up:

            # time elasped greater than threshold
            if datetime.now() - server2_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Server 2 might be down. Latest heartbeat received at {server2_time}\n")

                server2_up = False

        # server 3
        if server3_up:

            # time elasped greater than threshold
            if datetime.now() - server3_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Server 3 might be down. Latest heartbeat received at {server3_time}\n")
                
                server3_up = False

        # server 4
        if server4_up:

            # time elasped greater than threshold
            if datetime.now() - server4_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Server 4 might be down. Latest heartbeat received at {server4_time}\n")

                server4_up = False

        # wait to check again
        time.sleep(5)

        
# create server
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    heartbeat_service_pb2_grpc.add_ViewServiceServicer_to_server(ViewServiceServicer(), server)
    server.add_insecure_port("[::]:50056")

    try:
        print("Server started")
        server.start()

        check_thread = threading.Thread(target=check_servers, daemon=True)
        check_thread.start()

        server.wait_for_termination()

    except KeyboardInterrupt:
        print("\nServer shutting down")
        server.stop(0)


if __name__ == "__main__":
    serve()