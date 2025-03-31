# Coded by Mark Kiefer (251237385)
# CS 4459B - Assignment 2
# Due by March 11, 2025

import sys
import grpc
import replication_pb2
import replication_pb2_grpc

def run(key, value): 
    
    try:
        with grpc.insecure_channel('localhost:50051') as channel:

            # create stub
            stub = replication_pb2_grpc.SequenceStub(channel)

            # prepare write request
            request = replication_pb2.WriteRequest(key=key,value=value)

            # call write request
            response = stub.Write(request)

            # received ack
            if response.ack == "ack":

                # update log
                with open("client.txt", 'a') as file:
                    file.write(f"{key} {value}\n")

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