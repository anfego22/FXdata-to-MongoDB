#include "FXtoBSON.h"

using namespace std;


int main(int argc, char *argv[])
{
  /*
  mongo::client::initialize();
  DBClientConnection c;
  c.connect("localhost");
  */
  std::string file(argv[1]);
  FXtoBSON fxbson(file, "%Y%m%d%H%M%S");
  fxbson.printBSON();
  return EXIT_SUCCESS;
}


