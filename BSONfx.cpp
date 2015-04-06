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
BSONObj FXtoBSON::emptyDoc(const char &t){
  BSONObjBuilder emptyBB;
  switch(t){
  case 'h':
    {
      for(int i = 0; i < 60; i++){
	emptyBB.append(to_string(i), 0);
      }
      BSONObj HOUR = emptyBB.obj();
      return BSON("Open" << HOUR << "High" << HOUR <<
		  "Low" << HOUR << "Close" <<HOUR <<
		  "Vol" << HOUR);
    }
  case 'd':
    for(int i = 0; i < 24; i++){
      emptyBB.append(to_string(i), 0);
    }
    break;
  case 'm':
    for(int i = 1; i <= 31; i++){
      emptyBB.append(to_string(i), 0);
    }
    break;
  case 'y':
    for(int i = 0; i < 12; i++){
      emptyBB.append(to_string(i), 0);
    }
    break;
  }
  return emptyBB.obj();
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
  case 'y':
    tempTM.tm_min = 0;
    tempTM.tm_hour = 0;
    tempTM.tm_mday = 0;
    tempTM.tm_mon = 0;
    FIND.appendTimeT("Date", timegm(&tempTM));
    break;
  }
  return FIND.obj();
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
  int j = 0; 
  switch(a){
  case 'h':
    {
    int i = 59;
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
    }
    break;
  case 'd':
    {
    int i = 23;
    while(!Day(j,0))
      j++;
    reduc(0) = Day(j,0);
    reduc(1) = Day.col(1).maxCoeff();
    reduc(2) = Day.col(2).minCoeff();
    while(!Day(i, 3))
      i--;
    reduc(3) = Day(i, 3);
    reduc(4) = Day.col(4).sum();
    }
    break;
  case 'm':
    {
    int i = 30;
    while(!Month(j,0))
      j++;
    reduc(0) = Month(j,0);
    reduc(1) = Month.col(1).maxCoeff();
    reduc(2) = Month.col(2).minCoeff();
    while(!Month(i, 3))
      i--;
    reduc(3) = Month(i, 3);
    reduc(4) = Month.col(4).sum();
    }
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
    Year.row(tempTM.tm_mon) = reduc;
    break;
  }
  return BSON("Open" << reduc(0) <<
	      "High" << reduc(1) <<
	      "Low" << reduc(2) <<
	      "Close" << reduc(3) <<
	      "Vol" << reduc(4));
}

void FXtoBSON::addMinToDB(const struct tm &tempTM,
			   const BSONObj & document,
			   DBClientConnection & c){
  BSONObj FINDhour = find(tempTM, 'h');
  auto_ptr<DBClientCursor> cursor = c.query(dbH, FINDhour);
  if(cursor->more()){
    c.update(dbH , FINDhour, BSON("$set" << document));
  } else {
    c.update(dbH, FINDhour,
	     BSON("$set" << emptyDoc('h')), true);
    c.update(dbH, FINDhour, BSON("$set" << document));
  }
}

void FXtoBSON::updateDoc(const char &t, const struct tm &tempTM,
	       DBClientConnection &c){
  switch(t){
  case 'd':
    {
      BSONObj findHour = find(tempTM, 'h');
      BSONObj findDay = find(tempTM, 'd');
      auto_ptr<DBClientCursor> curH = c.query(dbH, findHour, 0, 0, &projId);
    if(curH->more()){
      c.update(dbD, findDay,
	       BSON("$set" << BSON(to_string(tempTM.tm_hour) <<
				   curH->next().getField("_id"))));
    } else {
      c.update(dbD, findDay,
	       BSON("$set" << emptyDoc(t)), true);
      c.update(dbD, findDay,
	       BSON("$set" << BSON(to_string(tempTM.tm_hour) <<
				   curH->next().getField("_id"))));
    }
  }
    break;
  case 'm':
    {
      BSONObj findDay = find(tempTM, 'd');
      BSONObj findMonth = find(tempTM, 'm');
    auto_ptr<DBClientCursor> curD = c.query(dbD, findDay, 0, 0, &projId);
    if(curD->more()){
      c.update(dbM, findMonth,
	       BSON("$set" << BSON(to_string(tempTM.tm_mday) <<
				   curD->next().getField("_id"))));
    } else {
      c.update(dbM, findMonth,
	       BSON("$set" << emptyDoc(t)), true);
      c.update(dbD, findMonth,
	       BSON("$set" << BSON(to_string(tempTM.tm_mday) <<
				   curD->next().getField("_id"))));
    }
    }
    break;
  case 'y':
    {
    BSONObj findMonth = find(tempTM, 'm');
    BSONObj findYear = find(tempTM, 'y');
    auto_ptr<DBClientCursor> curM = c.query(dbM, findMonth,0,0,&projId);
    if(curM->more()){
      c.update(dbY, findYear,
	       BSON("$set" << BSON(to_string(tempTM.tm_mday) <<
				   curM->next().getField("_id"))));
    } else {
      c.update(dbY, findYear,
	       BSON("$set" << emptyDoc(t)), true);
      c.update(dbY, findYear,
	       BSON("$set" << BSON(to_string(tempTM.tm_mon) <<
				   curM->next().getField("_id"))));
    }
    }
    break;
  } // switch end
}

void FXtoBSON::aggregateToDB(const char & t,
			     const struct tm & tempTM,
			     DBClientConnection &c){
  switch(t){
  case 'h':
    c.update(dbH, find(time0, 'h'),
	     BSON("$addToSet" << BSON("quote" <<
				      aggregate('h',time0))),true);
    Hour.setZero(60, 5);
    break;
  case 'd':
    c.update(dbD, find(tempTM, 'd'),
	     BSON("$addToSet" << BSON("quote" <<
				      aggregate('d', time0))), true); 
    Day.setZero(24, 5);
    break;
  case 'm':
    c.update(dbM, find(tempTM, 'm'),
	     BSON("$addToSet" << BSON("quote" <<
				      aggregate('m', time0))), true);
    Month.setZero();
    break;
  case 'y':
    c.update(dbM, find(tempTM, 'y'),
	     BSON("$addToSet" << BSON("quote" <<
				      aggregate('y', time0))), true);
    Year.setZero();
    break;
  }
}

FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   const string &pair, const string & source,
		   const char &sep_):
  file(file_), cols(0), rows(0), formatt(formatt_), sep(sep_){
  db = string("FOREX.") + source + string(".") + pair;
  dbH = db + string(".") + string("Hour");
  dbD = db + string(".") + string("Day");
  dbM = db + string(".") + string("Month");
  dbY = db + string(".") + string("Year");
  Hour.setZero(60, 5);
  Day.setZero(24, 5);
  Month.setZero(31, 5);
  Year.setZero(12, 5);
  mongo::client::initialize();
  DBClientConnection c;
  c.connect("localhost");
  string dropheader, line, checkEOF;
  struct tm tempTM;
  headers();
  getTime0();
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  projId = BSON("_id" << 1);

  while(getline(csvFile, line)){
    if(!line.empty()){
      BSONObj QUOTE = headerQuote(line, tempTM);
      BSONObj document = buildQuoteAt(tempTM.tm_min, QUOTE);
      addMinToDB(tempTM, document, c);
      if(tempTM.tm_hour == time0.tm_hour | time0.tm_hour == -1)
	hourToEigen(tempTM.tm_min, QUOTE); 
      if(tempTM.tm_hour != time0.tm_hour & time0.tm_hour != -1 | csvFile.peek() == -1){
	aggregateToDB('h', tempTM, c);
	hourToEigen(tempTM.tm_min, QUOTE);
    	updateDoc('d', time0, c);
      }
      if(tempTM.tm_mday != time0.tm_mday | csvFile.peek() == -1){
	aggregateToDB('d', tempTM, c);
	updateDoc('m', time0, c);
      }
      if(tempTM.tm_mon != time0.tm_mon | csvFile.peek() == -1){
	aggregateToDB('m', tempTM, c);
	updateDoc('y', time0, c);
      }
      if(tempTM.tm_year != time0.tm_year | csvFile.peek() == -1){
	aggregateToDB('y', time0, c);
      }
      time0 = tempTM;
    } // if end
  } // While end;

  csvFile.close();
}


