CC=g++
LIBS=-L/usr/include/mongo/lib -L/usr/share/boost-buil/ -pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex
IDIR =-I/usr/include/boost_1_57_0 -I/usr/include/mongo/include
CFLAGS= -w
OBJ = tutorial2.o BSONfx.o
DEP = FXtoBSON.h
all: TT

TT: $(OBJ) 
	$(CC) $^ -o $@ $(LIBS) $(IDIR) -std=c++11

%.o: %.cpp $(DEP) 
	$(CC) $(CFLAGS) $(IDIR) $(LIBS) -c -o $@ $< -std=c++11

clean:
	rm -rf *o TT
