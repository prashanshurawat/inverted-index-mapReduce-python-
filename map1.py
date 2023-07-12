import grpc
from concurrent import futures
import MapReduce_pb2
import MapReduce_pb2_grpc

word_map = {}


class map(MapReduce_pb2_grpc.map):
    def inputSplits(self, request, context, **kwargs):
        print("______________________________________________________")
        print("Split Received: \n", request.index_map)

        for index, words in request.index_map.items():
            for word in words.input:
                if word in word_map and int(index) not in word_map[word]:
                    word_map[word].append(int(index))
                else:
                    word_map[word] = [int(index)]

        response = MapReduce_pb2.input_response()
        response.response = "Input Splits Received by map 1"
        print("______________________________________________________")
        return response


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
MapReduce_pb2_grpc.add_mapServicer_to_server(map(), server)
server.add_insecure_port("[::]:50052")
server.start()
print("MAP 1 STARTED AT PORT 50052 ")
server.wait_for_termination(timeout=15)
print("50052 TERMINATED")

print("______________________________________________________")
print(word_map)
print("______________________________________________________")

r = 2
with open("map1/p0.txt", "w+") as file:
    for word, count in word_map.items():
        if len(word) % r == 0:
            file.write(str(word).lower() + ":")
            for index in count:
                file.write(" "+str(index))
            file.write("\n")
    file.close()

with open("map1/p1.txt", "w+") as file:
    for word, count in word_map.items():
        if len(word) % r == 1:
            file.write(str(word).lower() + ":")
            for index in count:
                file.write(" "+str(index))
            file.write("\n")
    file.close()
