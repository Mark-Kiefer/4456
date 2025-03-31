# Coded by Mark Kiefer (251237385)
# CS 4459B - Assignment 2
# Due by March 11, 2025

import sys
import grpc
import replication_pb2
import replication_pb2_grpc

def run(key, value): 
    
    try:

        # check if server 1 is primary
        with grpc.insecure_channel('localhost:50051') as channel:

            # create stub
            stub = replication_pb2_grpc.SequenceStub(channel)

            # prepare write request
            request = replication_pb2.WriteRequest(key=key,value=value)

            # call write request
            metadata = (("source", "client"),)
            response = stub.Write(request, metadata=metadata)

            # received ack
            if response.ack == "ack":

                # update log
                with open("client.txt", 'a') as file:
                    file.write(f"{key} {value}\n")

            if response.ack == "Nack":

                # check if server 2 is primary
                with grpc.insecure_channel('localhost:50052') as channel:

                    # create stub
                    stub = replication_pb2_grpc.SequenceStub(channel)

                    # prepare write request
                    request = replication_pb2.WriteRequest(key=key,value=value)

                    # call write request
                    metadata = (("source", "client"),)
                    response = stub.Write(request, metadata=metadata)

                    # received ack
                    if response.ack == "ack":

                        # update log
                        with open("client.txt", 'a') as file:
                            file.write(f"{key} {value}\n")

                    if response.ack == "Nack":

                        # check if server 3 is primary
                        with grpc.insecure_channel('localhost:50053') as channel:

                            # create stub
                            stub = replication_pb2_grpc.SequenceStub(channel)

                            # prepare write request
                            request = replication_pb2.WriteRequest(key=key,value=value)

                            # call write request
                            metadata = (("source", "client"),)
                            response = stub.Write(request, metadata=metadata)

                            # received ack
                            if response.ack == "ack":

                                # update log
                                with open("client.txt", 'a') as file:
                                    file.write(f"{key} {value}\n")

                            if response.ack == "Nack":

                                # check if server 4 is primary
                                with grpc.insecure_channel('localhost:50054') as channel:

                                    # create stub
                                    stub = replication_pb2_grpc.SequenceStub(channel)

                                    # prepare write request
                                    request = replication_pb2.WriteRequest(key=key,value=value)

                                    # call write request
                                    metadata = (("source", "client"),)
                                    response = stub.Write(request, metadata=metadata)

                                    # received ack
                                    if response.ack == "ack":

                                        # update log
                                        with open("client.txt", 'a') as file:
                                            file.write(f"{key} {value}\n")

                                    if response.ack == "Nack":  
                                        print("Error: Server is unavailable")   
                                                        
    # server not up
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            print("Error: Server is unavailable")

        else:
            print(f"Error: {e.code()}")



if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Need two arguments, key and value")
        exit(0)

    run(sys.argv[1], sys.argv[2])