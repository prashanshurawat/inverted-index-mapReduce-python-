from concurrent import futures
import grpc
import MapReduce_pb2
import MapReduce_pb2_grpc

map_locations = []
word_map = {}


class map(MapReduce_pb2_grpc.map):
    def reducer_inputs(self, request, context, **kwargs):
        print("______________________________________________________")
        print("location received by reducer 2: ", request.input)
        map_locations.extend(request.input)
        response = MapReduce_pb2.input_response(response="location received")
        print("______________________________________________________")
        return response


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
MapReduce_pb2_grpc.add_mapServicer_to_server(map(), server)
server.add_insecure_port("[::]:50056")
server.start()
print("REDUCER 2 STARTED AT PORT 50056")
server.wait_for_termination(timeout=13)
print("50056 TERMINATED")

for map in map_locations:
    with open(map + "/p1.txt", "r+") as file:
        data = file.read().split("\n")
        data.remove('')
        for indexes in data:
            word = indexes.split(':')[0]
            index = indexes.split(':')[1]
            if word in word_map:
                word_map[word].append(index)
            else:
                word_map[word] = [index]
        file.close()

print("______________________________________________________")
print(word_map)
print("______________________________________________________")

with open("output_files/output2.txt", "w+") as file:
    for word, count in word_map.items():
        file.write(word + " " + str(count) + "\n")
    file.close()
