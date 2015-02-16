CC=g++
LIBS=-L/usr/include/mongo/lib -L/usr/share/boost-buil/ -pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex
IDIR =-I/usr/include/boost_1_57_0 -I/usr/include/mongo/include
CFLAGS=-c -w
OBJ = tutorial2.o
all: TT

TT: $(OBJ) 
	$(CC) $^ -o $@ $(LIBS) $(IDIR) 

$(OBJ): tutorial2.cpp 
	$(CC) $(CFLAGS) $(IDIR) $(LIBS) $^ -std=c++11 

clean:
	rm -rf *o TT
