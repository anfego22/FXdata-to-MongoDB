#include "FXtoBSON.h"

using namespace std;
using namespace mongo;

void FXtoBSON::headers(){
  if(names.size() == 0){
    csvFile.seekg(0);
    csvFile.open(file.c_str());
    string cell, firstline;
    getline(csvFile, firstline);
    stringstream lineS(firstline);
    while(getline(lineS, cell, sep)){
      names.push_back(cell);
    }
    cols = names.size();
    csvFile.close();
  }
}

FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   const string &pair, DBClientConnection &c, const char &sep_):
  file(file_), cols(0), formatt(formatt_), h(0), sep(sep_){
  db = "FOREX.";
  db += pair;
  string dropheader, line, cell;
  int j = 0;
  struct tm tempTM;
  headers();
  csvFile.seekg(0);
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  while(getline(csvFile, line)){
    string query;
    BSONObjBuilder quotes, FIND;
    istringstream lineS(line);
    for (int i = 0; i!=cols; i++){
      getline(lineS, cell, sep);
      if (names[i] == "Date")
	strptime(cell.c_str(), formatt.c_str(), &tempTM);
      else{
	istringstream td(cell);
	double d;
	td >> d;
	quotes.append(names[i], d);
      }
    }
    string min = to_string(tempTM.tm_min);
    string hour = to_string(tempTM.tm_hour);
    string day = to_string(tempTM.tm_mday);
    string mon = to_string(tempTM.tm_mon);
    string p = ".";
    query = min;
    tempTM.tm_min = 0;
    FIND.appendTimeT("Date", mktime(&tempTM)); 
    try{
      c.update(db, FIND.obj(),
	       BSON("$addToSet" << BSON(query << quotes.obj())), true);
    } catch( const mongo::DBException &e) {
      std::cout << "caught " << e.what() << std::endl;
    }
  } // While end;
}

