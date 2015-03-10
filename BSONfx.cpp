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
    csvFile.seekg(0);
  }
}

void FXtoBSON::getTime0(){
  csvFile.open(file.c_str());
  string line, cell;
  getline(csvFile, line);// Drop Headers
  getline(csvFile, line);
  istringstream ss(line);
  for(int i = 0; i!= cols; i++){
    getline(ss, cell, sep);
    if(names[i] == "Date")
      strptime(cell.c_str(), formatt.c_str(), &time0);
    csvFile.close();
  }
  csvFile.seekg(0);
}

void FXtoBSON::headerQuote(const string &line, BSONObjBuilder &quotes,
			   struct tm &tempTM){
  string cell;
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
}

BSONObj FXtoBSON::buildQuoteAt(const int & min_, const BSONObj & QUOTE){
  string op, hi, lo, cl, vl, min;
  min = to_string(min_);
  op = string("Open.") + min;
  hi = string("High.") + min;
  lo = string("Low.") + min;
  cl = string("Close.") + min;
  vl = string("Vol.") + min;
  BSONObj doc;
  doc = BSON(op << QUOTE.getField("Open").numberDouble() <<
	     hi << QUOTE.getField("High").numberDouble() <<
	     lo << QUOTE.getField("Low").numberDouble() <<
	     cl << QUOTE.getField("Close").numberDouble() <<
	     vl << QUOTE.getField("Vol").numberDouble());
  return doc;
}

BSONObj FXtoBSON::emptyHour(){
  BSONObjBuilder hour;
  for(int i = 0; i< 60; i++){
    hour.append(to_string(i), 0);
  }
  BSONObj empty, HOUR;
  HOUR = hour.obj();
  empty = BSON("Open" << HOUR << "High" << HOUR <<
		   "Low" << HOUR << "Close" <<HOUR <<
		   "Vol" << HOUR);
  return empty;
}

FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   const string &pair, DBClientConnection &c, const char &sep_):
  file(file_), cols(0), rows(0), formatt(formatt_), sep(sep_){
  db = "FOREX.";
  db += pair;
  string dropheader, line;
  int j = 1;
  struct tm tempTM;
  BSONObj empty;
  headers();
  getTime0();
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  empty = emptyHour();
  while(getline(csvFile, line)){
    BSONObjBuilder quotes, FIND;
    headerQuote(line, quotes, tempTM);
    BSONObj QUOTE = quotes.obj();
    BSONObj document = buildQuoteAt(tempTM.tm_min, QUOTE);
    if(time0.tm_hour == tempTM.tm_hour){
      time0.tm_min = 0;
      FIND.appendTimeT("Date", timegm(&time0)); 
    } else {
      tempTM.tm_min = 0;
      FIND.appendTimeT("Date", timegm(&tempTM));
    }
    try{
      auto_ptr<DBClientCursor> cursor = c.query(db, FIND.asTempObj());
      if(cursor->more()){
	c.update(db, FIND.obj(), BSON("$set" << document));
      } else {
	c.update(db, FIND.asTempObj(), 
		 BSON("$set" << empty), true);
	c.update(db, FIND.obj(), BSON("$set" << document));
      }
    } catch( const mongo::DBException &e) {
      std::cout << "caught " << e.what() << std::endl;
    }
    time0 = tempTM;
  } // While end;
}


