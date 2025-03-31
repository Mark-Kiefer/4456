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
primary_time = ""
backup_time = ""
primary_up = False
backup_up = False

class ViewServiceServicer(heartbeat_service_pb2_grpc.ViewServiceServicer):

    # function receives heartbeats from primary and backup and logs them
    def Heartbeat(self, request, context):

        global primary_time, backup_time, primary_up, backup_up

        identifier = request.service_identifier[0].upper() + request.service_identifier[1:]
        time = datetime.now()

        # update last known times
        if identifier == "Primary":
            primary_time = time
            primary_up = True

        if identifier == "Backup":
            backup_time = time
            backup_up = True

        # update log
        with open("heartbeat.txt", 'a') as file:
            file.write(f"{identifier} is alive. Latest heartbeat received at {time}\n")

        return empty_pb2.Empty() 


# repeatly checks on servers
def check_servers():

    global primary_time, backup_time, primary_up, backup_up

    while True:

        # primary server
        if primary_up:

            # time elasped greater than threshold
            if datetime.now() - primary_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Primary might be down. Latest heartbeat received at {primary_time}\n")
                
                primary_up = False

        # backup server 
        if backup_up:

            # time elasped greater than threshold
            if datetime.now() - backup_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Backup might be down. Latest heartbeat received at {backup_time}\n")

                backup_up = False

        # wait to check again
        time.sleep(5)

        
# create server
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    heartbeat_service_pb2_grpc.add_ViewServiceServicer_to_server(ViewServiceServicer(), server)
    server.add_insecure_port("[::]:50053")

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