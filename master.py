import grpc
import MapReduce_pb2
import MapReduce_pb2_grpc
import re
import time

input_path = "input_files/"

file1 = input_path + "input1.txt"
file2 = input_path + "input2.txt"
file3 = input_path + "input3.txt"

files = [file1, file2, file3]
maps = ["50052", "50053", "50054"]

i = 1
for file in files:
    with open(file, "r") as f:
        data = f.read()

    s = re.split(" |\n", data)

    split_size = len(s) // 3
    Splits = [s[i:i + split_size] for i in range(0, len(s), split_size)]
    indexes = []
    for split in Splits:
        l = MapReduce_pb2.input_split(input=split)
        map = {str(i): l}
        indexes.append(map)
    print(indexes)

    for chunk, map in zip(indexes, maps):
        channel = grpc.insecure_channel('localhost:{0}'.format(map))
        server_stub = MapReduce_pb2_grpc.mapStub(channel)
        request = MapReduce_pb2.input_map(index_map=chunk)
        response = server_stub.inputSplits(request)
        print(response.response)

    i += 1

time.sleep(15)

reducers = ["50055", "50056"]
for reducer in reducers:
    channel = grpc.insecure_channel('localhost:'+reducer)
    server_stub = MapReduce_pb2_grpc.mapStub(channel)
    request = MapReduce_pb2.input_split(input=["map1", "map2", "map3"])
    response = server_stub.reducer_inputs(request)
    print(response.response)
