# Coded by Mark Kiefer (251237385)
# CS 4459B - Assignment 2
# Due by March 11, 2025

import grpc
import replication_pb2
import replication_pb2_grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
from google.protobuf import empty_pb2
from concurrent import futures
from datetime import datetime, timedelta
import time
import threading

# constant
SERVER_DOWN_THRESHOLD = 10

# global variables for recording last known time
server1_time = ""
server2_time = ""
server3_time = ""
server4_time = ""
server1_up = False
server2_up = False
server3_up = False
server4_up = False
primary = "" 

class ViewServiceServicer(heartbeat_service_pb2_grpc.ViewServiceServicer):

    # function receives heartbeats from primary and backup and logs them
    def Heartbeat(self, request, context):

        global server1_time, server2_time, server3_time, server4_time, server1_up, server2_up, server3_up, server4_up, primary

        identifier = request.service_identifier[0].upper() + request.service_identifier[1:]
        time = datetime.now()

        # update last known times
        if identifier == "Server_1":
            server1_time = time

            if not server1_up and not server2_up and not server3_up and not server4_up:

                try:

                    with grpc.insecure_channel('localhost:50051') as channel:

                        # create stub
                        stub = replication_pb2_grpc.SequenceStub(channel)

                        # prepare write request
                        request = replication_pb2.WriteRequest(key="0",value="0")

                        # call write request
                        metadata = (("source", "heartbeat"),)
                        response = stub.Write(request, metadata=metadata)

                        # received ack
                        if response.ack == "ack":
                            primary = "1"
                            print("Server 1 is primary")

                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        print("Error: Server is unavailable")

                    else:
                        print(f"Error: {e.code()}")

            server1_up = True

        if identifier == "Server_2":
            server2_time = time

            if not server1_up and not server2_up and not server3_up and not server4_up:

                try:

                    with grpc.insecure_channel('localhost:50052') as channel:

                        # create stub
                        stub = replication_pb2_grpc.SequenceStub(channel)

                        # prepare write request
                        request = replication_pb2.WriteRequest(key="0",value="0")

                        # call write request
                        metadata = (("source", "heartbeat"),)
                        response = stub.Write(request, metadata=metadata)

                        # received ack
                        if response.ack == "ack":
                            primary = "2"
                            print("Server 2 is primary")

                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        print("Error: Server is unavailable")

                    else:
                        print(f"Error: {e.code()}")

            server2_up = True

        if identifier == "Server_3":
            server3_time = time

            if not server1_up and not server2_up and not server3_up and not server4_up:

                try:

                    with grpc.insecure_channel('localhost:50053') as channel:

                        # create stub
                        stub = replication_pb2_grpc.SequenceStub(channel)

                        # prepare write request
                        request = replication_pb2.WriteRequest(key="0",value="0")

                        # call write request
                        metadata = (("source", "heartbeat"),)
                        response = stub.Write(request, metadata=metadata)

                        # received ack
                        if response.ack == "ack":
                            primary = "3"
                            print("Server 3 is primary")

                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        print("Error: Server is unavailable")

                    else:
                        print(f"Error: {e.code()}")

            server3_up = True

        if identifier == "Server_4":
            server4_time = time

            if not server1_up and not server2_up and not server3_up and not server4_up:

                try:

                    with grpc.insecure_channel('localhost:50054') as channel:

                        # create stub
                        stub = replication_pb2_grpc.SequenceStub(channel)

                        # prepare write request
                        request = replication_pb2.WriteRequest(key="0",value="0")

                        # call write request
                        metadata = (("source", "heartbeat"),)
                        response = stub.Write(request, metadata=metadata)

                        # received ack
                        if response.ack == "ack":
                            primary = "4"
                            print("Server 4 is primary")

                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        print("Error: Server is unavailable")

                    else:
                        print(f"Error: {e.code()}")

            server4_up = True

        # update log
        with open("heartbeat.txt", 'a') as file:
            file.write(f"{identifier} is alive. Latest heartbeat received at {time}\n")

        return empty_pb2.Empty() 


# repeatly checks on servers
def check_servers():

    global server1_time, server2_time, server3_time, server4_time, server1_up, server2_up, server3_up, server4_up, primary

    while True:

        election = False

        # server 1
        if server1_up:

            # time elasped greater than threshold
            if datetime.now() - server1_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Server 1 might be down. Latest heartbeat received at {server1_time}\n")
                
                server1_up = False
                if primary == "1":
                    election = True

        # server 2
        if server2_up:

            # time elasped greater than threshold
            if datetime.now() - server2_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Server 2 might be down. Latest heartbeat received at {server2_time}\n")

                server2_up = False

                if primary == "2":
                    election = True

        # server 3
        if server3_up:

            # time elasped greater than threshold
            if datetime.now() - server3_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Server 3 might be down. Latest heartbeat received at {server3_time}\n")
                
                server3_up = False

                if primary == "3":
                    election = True

        # server 4
        if server4_up:

            # time elasped greater than threshold
            if datetime.now() - server4_time > timedelta(seconds=SERVER_DOWN_THRESHOLD):
                with open("heartbeat.txt", 'a') as file:
                    file.write(f"Server 4 might be down. Latest heartbeat received at {server4_time}\n")

                server4_up = False

                if primary == "4":
                    election = True

        # election
        if election:

            try:

                if server1_up:

                    with grpc.insecure_channel('localhost:50051') as channel:

                            # create stub
                            stub = replication_pb2_grpc.SequenceStub(channel)

                            # prepare write request
                            request = replication_pb2.WriteRequest(key="0",value="0")

                            # call write request
                            metadata = (("source", "heartbeat"),)
                            response = stub.Write(request, metadata=metadata)

                            # received ack
                            if response.ack == "ack":
                                server1_up = True
                                primary = "1"
                                print("Server 1 is primary")

                elif server2_up:

                    with grpc.insecure_channel('localhost:50052') as channel:

                            # create stub
                            stub = replication_pb2_grpc.SequenceStub(channel)

                            # prepare write request
                            request = replication_pb2.WriteRequest(key="0",value="0")

                            # call write request
                            metadata = (("source", "heartbeat"),)
                            response = stub.Write(request, metadata=metadata)

                            # received ack
                            if response.ack == "ack":
                                server2_up = True
                                primary = "2"
                                print("Server 2 is primary")

                elif server3_up:

                    with grpc.insecure_channel('localhost:50053') as channel:

                            # create stub
                            stub = replication_pb2_grpc.SequenceStub(channel)

                            # prepare write request
                            request = replication_pb2.WriteRequest(key="0",value="0")

                            # call write request
                            metadata = (("source", "heartbeat"),)
                            response = stub.Write(request, metadata=metadata)

                            # received ack
                            if response.ack == "ack":
                                server3_up = True
                                primary = "3"
                                print("Server 3 is primary")

                elif server4_up:

                    with grpc.insecure_channel('localhost:50054') as channel:

                            # create stub
                            stub = replication_pb2_grpc.SequenceStub(channel)

                            # prepare write request
                            request = replication_pb2.WriteRequest(key="0",value="0")

                            # call write request
                            metadata = (("source", "heartbeat"),)
                            response = stub.Write(request, metadata=metadata)

                            # received ack
                            if response.ack == "ack":
                                server4_up = True
                                primary = "4"
                                print("Server 4 is primary")

            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Error: Server is unavailable")

                else:
                    print(f"Error: {e.code()}")

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