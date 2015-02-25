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
    while(getline(lineS, cell, ';')){
      names.push_back(cell);
    }
    cols = names.size();
    csvFile.close();
  }
}

void FXtoBSON::Years(){
  if(T == 0){
    csvFile.seekg(0);
    string line, cell;
    struct tm tmOne, tmLast;
    getline(csvFile, line);
    getline(csvFile, line);
    istringstream lineOne(line);
    for (int i = 0; i!= cols; i++){
      getline(lineOne, cell, ';');
      if(names[i] == "Date")
	strptime(cell.c_str(), formatt.c_str(), &tmOne);
      else
	;
    }
    while(getline(csvFile, line)){
      istringstream lineLast(line);
      for (int i = 0; i!= cols; i++){
	getline(lineLast, cell, ';');
	if(names[i] == "Date")
	strptime(cell.c_str(), formatt.c_str(), &tmLast);
	else
	  ;
      }
    }
    T = tmLast.tm_year - tmOne.tm_year + 1;
  }
}


FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   const string &pair, DBClientConnection &c):
  file(file_), T(0), cols(0), formatt(formatt_), h(0){
  db = "FOREX.";
  db += pair;
  string dropheader, line, cell;
  int j = 0;
  struct tm tempTM;
  headers();
  Years();
  csvFile.seekg(0);
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  while(getline(csvFile, line)){
    string query;
    BSONObjBuilder quotes;
    istringstream lineS(line);
    for (int i = 0; i!=cols; i++){
      getline(lineS, cell, ';');
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
    query = mon + p + day + p + hour + p + min ;
    try{
      c.update(db, BSON("Year" << tempTM.tm_year + 1900),
	       BSON("$addToSet" << BSON(query << quotes.obj())), true);
    } catch( const mongo::DBException &e) {
      std::cout << "caught " << e.what() << std::endl;
    }
  } // While end;
}

