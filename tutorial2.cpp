#include "FXtoBSON.h"
#include "mongo/client/dbclient.h" // for the driver
#include <cstdlib>

using namespace std;
using namespace mongo;

int main(int argc, char *argv[])
{
  mongo::client::initialize();
  try {
    mongo::DBClientConnection c;
    c.connect("localhost");
  } catch( const mongo::DBException &e ) {
    std::cout << "caught " << e.what() << std::endl;
  }
  std::string file(argv[1]);
  FXtoBSON fxbson(file, "%Y%m%d%H%M%S");
  fxbson.printBSON();
  return EXIT_SUCCESS;

}


