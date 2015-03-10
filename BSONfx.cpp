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

BSONObj FXtoBSON::headerQuote(const string &line, struct tm &tempTM){
  BSONObjBuilder quotes;
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
  return quotes.obj();
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

BSONObj FXtoBSON::dayDoc(const struct tm &tempTM){
  BSONObjBuilder dayId;
  struct tm tm2;
  tm2.tm_hour = 0;
  for(int i = 0; i < 24; i++){
    dayId.append(to_string(i), 0);
  }
  dayId.appendTimeT("Date", timegm(&tm2));
  BSONObj emptyDay = dayId.obj();
  return emptyDay;
}

BSONObj FXtoBSON::find(struct tm tempTM, const int &a){
  BSONObjBuilder FIND;
  switch(a){
  case 1:
    tempTM.tm_min = 0;
    FIND.appendTimeT("Date", timegm(&tempTM));
    break;
  case 2:
    tempTM.tm_min = 0;
    tempTM.tm_hour = 0;
    FIND.appendTimeT("Date", timegm(&tempTM));
    break;
  case 3:
    tempTM.tm_min = 0;
    tempTM.tm_hour = 0;
    tempTM.tm_mday = 0;
    FIND.appendTimeT("Date", timegm(&tempTM));
    break;
  }
  return FIND.obj();
}


FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   const string &pair, const string & source,
		   const char &sep_):
  file(file_), cols(0), rows(0), formatt(formatt_), sep(sep_){
  db = string("FOREX.") + source + string(".") + pair;
  dbH = db + string(".") + string("hour");
  dbD = db + string(".") + string("Day");
  dbM = db + string(".") + string("Month");
  mongo::client::initialize();
  DBClientConnection c;
  c.connect("localhost");
  string dropheader, line;
  struct tm tempTM;
  headers();
  getTime0();
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  BSONObj empty = emptyHour();
  BSONObj projId = BSON("_id" << 1);
  while(getline(csvFile, line)){
    BSONObj QUOTE = headerQuote(line, tempTM);
    BSONObj document = buildQuoteAt(tempTM.tm_min, QUOTE);
    BSONObj FINDhour = find(tempTM, 1);
    auto_ptr<DBClientCursor> cursor = c.query(dbH, FINDhour);
    if(cursor->more()){
      c.update(dbH , FINDhour, BSON("$set" << document));
    } else {
      c.update(dbH, FINDhour,
	       BSON("set" << empty), true);
      c.update(dbH, FINDhour, BSON("$set" << document));
    }
    if(time0.tm_hour != tempTM.tm_hour){
      BSONObj FINDday = FIND(tempTM, 2);
      auto_ptr<DBClientCursor> curD = c.query(dbD, FINDday);
      auto_ptr<DBClientCursor> curH = c.query(dbH, find(time0, 1));
      if(curD->more()){
	c.update(dbD, FINDday,
		 BSON("$set" << to_string(time0.tm_hour) <<
		      curH->next().getField("_id").OID()));
      } else {
	c.update(dbD, FINDday,
		 BSON("$set" << dayDoc(time0)), true);
	c.update(dbD, FINDday,
		 BSON("$set" << to_string(time0.tm_hour) <<
		      curH->next().getField("_id").OID()));
      }
    }
    time0 = tempTM;
  } // While end;
}


