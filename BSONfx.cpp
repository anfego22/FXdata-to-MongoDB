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
  time0.tm_hour = -1;
  csvFile.seekg(0);
}
// read the line and organize the values
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

// Construct the BSON with the field and value with respective minute of the hour
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
//Creates a document for an hour
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
// create the document that will hold the ID of 24 hours documents
BSONObj FXtoBSON::dayDoc(){
  BSONObjBuilder dayId;
  for(int i = 0; i < 24; i++){
    dayId.append(to_string(i), 0);
  }
  BSONObj emptyDay = dayId.obj();
  return emptyDay;
}

// Create the date of hour/day/month document
BSONObj FXtoBSON::find(struct tm tempTM, const char &a){
  BSONObjBuilder FIND;
  switch(a){
  case 'h':
    tempTM.tm_min = 0;
    FIND.appendTimeT("Date", timegm(&tempTM));
    break;
  case 'd':
    tempTM.tm_min = 0;
    tempTM.tm_hour = 0;
    FIND.appendTimeT("Date", timegm(&tempTM));
    break;
  case 'm':
    tempTM.tm_min = 0;
    tempTM.tm_hour = 0;
    tempTM.tm_mday = 0;
    FIND.appendTimeT("Date", timegm(&tempTM));
    break;
  }
  return FIND.obj();
}

// Look if hour has changed to add that id to the document
void FXtoBSON::updateDay(const struct tm &tempTM,
			 DBClientConnection &c){
  if(time0.tm_hour != tempTM.tm_hour){
    BSONObj FINDday = find(tempTM, 2);
    auto_ptr<DBClientCursor> curD = c.query(dbD, FINDday);
    auto_ptr<DBClientCursor> curH = c.query(dbH, find(tempTM, 1));
    if(curD->more()){
      if(curH->more())
	c.update(dbD, FINDday,
		 BSON("$set" << BSON(to_string(tempTM.tm_hour) <<
				     curH->next().getField("_id"))));
    } else {
      c.update(dbD, FINDday,
	       BSON("$set" << dayDoc()), true);
      c.update(dbD, FINDday,
	       BSON("$set" << BSON(to_string(tempTM.tm_hour) <<
				   curH->next().getField("_id"))));
    }
  }
}

// add a document containing a quote
void FXtoBSON::hourToEigen(const int &j, const BSONObj &QUOTE){
  Hour(j,0) = QUOTE.getField("Open").numberDouble();
  Hour(j,1) = QUOTE.getField("High").numberDouble();
  Hour(j,2) = QUOTE.getField("Low").numberDouble();
  Hour(j,3) = QUOTE.getField("Close").numberDouble();
  Hour(j,4) = QUOTE.getField("Vol").numberDouble();
}

VectorXd FXtoBSON::reduce(const char &a){
  VectorXd reduc(5);
  int j = 0; int i = 59;
  switch(a){
  case 'h':
    while(!Hour(j,0))
      j++;
    reduc(0) = Hour(j,0);
    reduc(1) = Hour.col(1).maxCoeff();
    for(int k = 0; k < 60; k++){
      if(Hour(k, 2) == 0)
	Hour(k, 2) = 100;
    }
    reduc(2) = Hour.col(2).minCoeff();
    while(!Hour(i, 3))
      i--;
    reduc(3) = Hour(i, 3);
    reduc(4) = Hour.col(4).sum();
    break;
  case 'd':
    while(!Day(j,0))
      j++;
    reduc(0) = Day(j,0);
    reduc(1) = Day.col(1).maxCoeff();
    reduc(2) = Day.col(2).minCoeff();
    while(!Day(i, 3))
      i--;
    reduc(3) = Day(i, 3);
    reduc(4) = Day.col(4).sum();
    break;
  case 'm':
    while(!Month(j,0))
      j++;
    reduc(0) = Month(j,0);
    reduc(1) = Month.col(1).maxCoeff();
    reduc(2) = Month.col(2).minCoeff();
    while(!Month(i, 3))
      i--;
    reduc(3) = Month(i, 3);
    reduc(4) = Month.col(4).sum();
    break;
  }
  return reduc;
}

BSONObj FXtoBSON::aggregate(const char &a, const struct tm &tempTM){
  VectorXd reduc(5);
  switch(a){
  case 'h':
    reduc = reduce('h');
    Day.row(tempTM.tm_hour) = reduc;
    break;
  case 'd':
    reduc = reduce('d');
    Month.row(tempTM.tm_mday) = reduc;
    break;
  case 'm':
    reduc = reduce('m');
    //Year.row(tempTM.tm_mon) = reduc;
    break;
  }
  return BSON("Open" << reduc(0) <<
	     "High" << reduc(1) <<
	     "Low" << reduc(2) <<
	     "Close" << reduc(3) <<
	     "Vol" << reduc(4));
}

FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   const string &pair, const string & source,
		   const char &sep_):
  file(file_), cols(0), rows(0), formatt(formatt_), sep(sep_){
  db = string("FOREX.") + source + string(".") + pair;
  dbH = db + string(".") + string("Hour");
  dbD = db + string(".") + string("Day");
  dbM = db + string(".") + string("Month");
  Hour.setZero(60, 5);
  Day.setZero(24, 5);
  mongo::client::initialize();
  DBClientConnection c;
  c.connect("localhost");
  string dropheader, line;
  struct tm tempTM;
  headers();
  getTime0();
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  int j = 0;
  BSONObj empty = emptyHour();
  BSONObj projId = BSON("_id" << 1);
  while(getline(csvFile, line)){
    BSONObj QUOTE = headerQuote(line, tempTM);
    BSONObj document = buildQuoteAt(tempTM.tm_min, QUOTE);
    BSONObj FINDhour = find(tempTM, 'h');
    auto_ptr<DBClientCursor> cursor = c.query(dbH, FINDhour);
    if(cursor->more()){
      c.update(dbH , FINDhour, BSON("$set" << document));
    } else {
      c.update(dbH, FINDhour,
	       BSON("$set" << empty), true);
      c.update(dbH, FINDhour, BSON("$set" << document));
    }
    updateDay(tempTM, c);
    if(tempTM.tm_hour == time0.tm_hour | time0.tm_hour == -1)
      hourToEigen(tempTM.tm_min, QUOTE); 
    if(tempTM.tm_hour != time0.tm_hour && time0.tm_hour != -1){
      c.update(dbH, find(time0, 'h'),
	       BSON("$addToSet" << BSON("quote" <<
					aggregate('h', time0))));
      Hour.setZero(60, 5);
      hourToEigen(tempTM.tm_min, QUOTE);
    }
    if(tempTM.tm_mday != time0.tm_mday){
      c.update(dbD, find(tempTM, 'd'),
	       BSON("$addToSet" << BSON("quote" <<
					aggregate('m', time0)))); 
      Day.setZero(24, 5);
    }
    time0 = tempTM;
  } // While end;
  
}


