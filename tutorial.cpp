#include <cstdlib>
#include <iostream>
#include "mongo/client/dbclient.h" // for the driver

//g++ tutorial.cpp -pthread -lmongoclient -lboost_thread -lboost_system -lboost_regex -I/usr/include/boost_1_57_0 -I/usr/include/mongo/include -L/usr/include/mongo/lib -L/usr/share/boost-buil/ -o tutorial

void run() {
  mongo::DBClientConnection c;
  c.connect("localhost");
}

int main() {
    mongo::client::initialize();
    try {
        run();
        std::cout << "connected ok" << std::endl;
    } catch( const mongo::DBException &e ) {
        std::cout << "caught " << e.what() << std::endl;
    }
    BSONObjBuilder b;
    b.append("name", "Joe");
    b.append("age", 33);
    BSONObj p = b.obj();
    /* Same as:
    BSONObj p = BSONObjBuilder().append("name", "Joe").append("age", 33).obj();
    BSONObjBuilder b;
    b << "name" << "Joe" << "age" << 33;
    Useful for a loop!
    GENOI generate the id of the object, it does even if it's excluded but 
    BSONObj p = BSON(GENOID, "name" << "Joe" << "age" << 33);
    OR
    BSONObj p = BSONObjBuilder().genOID().append("name", "Joe").obj();
    */
    // Now we insert p in a personal database (database test, in document driver)
    c.insert("test.driver", p);
    /* Remember c is an DBClientConnection object, created in the run function
       In order to see if we insert p in the database we should call last error
    */
    string error = mongo::DBClientWithCommands::getLastError();
    if(error){
      cout << error << endl;
      return 0;
    }
    cout << "Look your database " << endl
    return EXIT_SUCCESS;
}
