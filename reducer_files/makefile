CC = g++
CFLAGS = -g -Wall -std=c++1z
DUMMY_MAPPER_SRC = utilities.cpp reducer_server.cpp dummy_mapper.cpp
DUMMY_REDUCER_SRC = utilities.cpp super_reducer_client.cpp dummy_reducer.cpp
REDUCER_SRC = utilities.cpp reducer_worker.cpp
SUPER_REDUCER_SRC = utilities.cpp super_reducer_worker.cpp

all: dummy_mapper dummy_reducer reducer super_reducer

dummy_mapper: $(DUMMT_MAPPER_SRC)
	$(CC) $(CFLAGS) -o $@ $(DUMMY_MAPPER_SRC)

dummy_reducer: $(DUMMY_REDUCER_SRC)
	$(CC) $(CFLAGS) -o $@ $(DUMMY_REDUCER_SRC)

reducer: $(REDUCER_SRC)
	$(CC) $(CFLAGS) -o $@ $(REDUCER_SRC) -lpthread

super_reducer: $(SUPER_REDUCER_SRC)
	$(CC) $(CFLAGS) -o $@ $(SUPER_REDUCER_SRC) -lpthread

clean:
	rm dummy_mapper dummy_reducer reducer super_reducer
