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

void FXtoBSON::rowFile(){
  if(rows == 0){
    csvFile.seekg(0);
    csvFile.open(file.c_str());
    string line, cell;
    getline(csvFile, line);// Drop Headers
    getline(csvFile, line);
    istringstream ss(line);
    for(int i = 0; i!= cols; i++){
      getline(ss, cell, sep);
      if(names[i] == "Date")
	strptime(cell.c_str(), formatt.c_str(), &time0);
    }
    while(getline(csvFile, line)){
      rows++;
    }
    rows += 1; // Headers not included
    csvFile.close();
  }
}

FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   const string &pair, DBClientConnection &c, const char &sep_):
  file(file_), cols(0), rows(0), formatt(formatt_), sep(sep_){
  db = "FOREX.";
  db += pair;
  string dropheader, line, cell;
  int j = 1;
  struct tm tempTM;
  headers();
  rowFile();
  csvFile.seekg(0);
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  while(getline(csvFile, line)){
    string query;
    BSONObjBuilder quotes, FIND, Open, High, Low, Close, Vol;
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
    BSONObj QUOTE = quotes.obj();
    string min = to_string(tempTM.tm_min);
    Open.append(min, QUOTE.getField("Open").numberDouble());
    High.append(min, QUOTE.getField("High").numberDouble());
    Low.append(min, QUOTE.getField("Low").numberDouble());
    Close.append(min, QUOTE.getField("Close").numberDouble());
    Vol.append(min, QUOTE.getField("Vol").numberDouble());
    if(time0.tm_hour == tempTM.tm_hour){
      time0.tm_min = 0;
      FIND.appendTimeT("Date", timegm(&time0)); 
    } else {
      tempTM.tm_min = 0;
      FIND.appendTimeT("Date", timegm(&tempTM));
    }
    try{
      c.update(db, FIND.obj(),
	       BSON("$addToSet" << BSON("Open" << Open.obj() <<
					"High" << High.obj() <<
					"Low" << Low.obj() <<
					"Close" << Close.obj() <<
					"Vol" << Vol.obj())), true);
    } catch( const mongo::DBException &e) {
	std::cout << "caught " << e.what() << std::endl;
    }
    time0 = tempTM;
  } // While end;
}


