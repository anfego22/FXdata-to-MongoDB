CC=g++
LIBS=-L/usr/include/mongo/lib -L/usr/share/boost-buil/ -pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex
IDIR =-I/usr/include/boost_1_57_0 -I/usr/include/mongo/include
CFLAGS=-c -w
OBJ = tutorial2
DEP = FXtoBSON.h
all: TT

TT: $(OBJ).o $(DEP)
	$(CC) $(OBJ).cpp -o $@ $(LIBS) $(IDIR) -std=c++11

$(OBJ).o: $(OBJ).cpp $(DEP) 
	$(CC) $(CFLAGS) $(IDIR) $(LIBS) $(OBJ).cpp -o $@ -std=c++11 

clean:
	rm -rf *o TT
