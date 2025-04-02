# Coded by Mark Kiefer (251237385)
# CS 4459B - Assignment 2
# Due by March 11, 2025

import sys
import grpc
import replication_pb2
import replication_pb2_grpc

def connect(server, key, value):

    try:

        with grpc.insecure_channel(server) as channel:

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

                return response
        
    # server not up
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            print("Error: Server is unavailable")

        else:
            print(f"Error: {e.code()}")


def run(key, value): 

    if connect('localhost:50051', key, value).ack == "Nack":
        if connect('localhost:50052', key, value).ack == "Nack":
            if connect('localhost:50053', key, value).ack == "Nack":
                if connect('localhost:50054', key, value).ack == "Nack":
                    print("Error: Server is unavailable")  


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Need two arguments, key and value")
        exit(0)

    run(sys.argv[1], sys.argv[2])