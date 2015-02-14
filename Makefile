CC=g++
LIBS=-L/usr/include/mongo/lib -L/usr/share/boost-buil/ -pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex
IDIR =-I/usr/include/boost_1_57_0 -I/usr/include/mongo/include
CFLAGS=-c -w
OBJ = tutorial.o
all: TT

TT: $(OBJ) 
	$(CC) $^ -o $@ $(LIBS) $(IDIR) 

%.o: %.cpp 
	$(CC) $(CFLAGS) $(IDIR) $(LIBS) $^ 

clean:
	rm -rf *o TT
