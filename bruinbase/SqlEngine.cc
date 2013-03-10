/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
#include <vector>
#include <algorithm>
#include <fstream>
#include <set>
#define TESTING 0

using namespace std;

// external functions and variables for load file and sql command parsing 
typedef struct {
  int       key;  // slot number. the first slot is 0
  RecordId  rid;  // page number. the first page is 0
} IndexEntry;


bool operator<(const IndexEntry& a, const IndexEntry& b) {
	return (a.key<b.key);
}

extern FILE* sqlin;
int sqlparse(void);

void getRidsFirstCond(SelCond condition, BTreeIndex& idx, set<IndexEntry>& resultsToCheck);
void filterKeys(const vector<SelCond>& conds, set<IndexEntry>& results);
bool valueSatisfiesConds(string& value, const vector<SelCond>& conds);
void getRidsInRange(SelCond& lowerBound, SelCond& upperBound, BTreeIndex& idx, set<IndexEntry>& resultsToCheck);
bool filterConds(vector<SelCond>& conds, SelCond& lowerBound, SelCond& upperBound);


//helper function to compare key conditions (EQ > GT/GE > LT/LE > NE)
bool compareKeyConds (const SelCond& a, const SelCond& b) {
    switch (a.comp) {
        case SelCond::EQ:
            return true;
	        break;
        case SelCond::GE:
        case SelCond::GT:
            if (b.comp == SelCond::EQ)
                return false;
            else if (b.comp == SelCond::GE || b.comp == SelCond::GT) {
		        if (atoi(a.value) < atoi(b.value))
			        return false;
		        else if (atoi(a.value) == atoi(b.value)) { // break ties between two GT/GEs
		            if (a.comp == SelCond::GE && b.comp == SelCond::GT) // choose b when 1) a: >=   b: >
		                return false;
		            else        // choose a when 1) a: >=    b: >=
		                        //               2) a: >     b: >
		                        //               3) a: >     b: >=
		                return true;
		        }
		        else
		            return true;
	        }
    	    else
                return true;
	        break;
    case SelCond::LT:
    case SelCond::LE:
        if (b.comp == SelCond::EQ || b.comp == SelCond::GE || b.comp == SelCond::GT)
            return false;
        else if (b.comp == SelCond::LT || b.comp == SelCond::LE) {
		    if (atoi(a.value) > atoi(b.value))
			    return false;
			else if (atoi(a.value) == atoi(b.value)) { // break ties between two LT/LEs
			    if (a.comp == SelCond::LE && b.comp == SelCond::LT) // choose b when 1) a: <=   B: <
			        return false;
			    else    // choose a when 1) a: <=   b: <=
			            //               2) a: <    b: <
			            //               3) a: <    B: <=
			        return true;
			}
			else
			    return true;
	    }
        else
            return true;
        break;
    default:
        return false;
  }
} 


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}


RC SqlEngine::select(int attr, const string &table, const vector<SelCond>&conds) {

    RecordFile rf;   // RecordFile containing the table

    RC     rc;
    bool   indexExists;
    int    key;
    string value;
    

    // OPEN FILES
    // open the table file
    if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
        //fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
        return rc;
    }


    // open the index file -- if it doesn't exist, perform linearScan
    BTreeIndex index;
    if (index.open(table + ".idx", 'r') < 0) {
        //cout << "no index available, calling linear scan" << endl << endl;
        linearScan(attr, rf, conds);
        rf.close();
        return 0;
    }

	vector<SelCond> keyConds, valueConds;


    // PREPARE SELECT CONDITIONS
    // separate conds into keyConds (sorted) or valueConds
    // scan entire table if no conditions
    if (conds.empty()) {
        if (attr == 2 || attr == 3) {  // value or *	
		    //cout << "no conditions, calling linear scan" << endl << endl;
            linearScan(attr, rf, conds);
            index.close();
			rf.close();
            return 0;
        }
        else {
		    SelCond condition;
		    condition.comp=SelCond::GE;
		    condition.attr=1;
            char condValue = '0';
		    condition.value = &condValue;
    	    keyConds.push_back(condition);
        }
	}

    for (int i = 0; i < conds.size(); i++)
        if (conds[i].attr == 1)
            keyConds.push_back(conds[i]);
        else
            valueConds.push_back(conds[i]);
    //cout << "beginning sort" << endl;
    sort(keyConds.begin(), keyConds.end(), compareKeyConds);
    

    // if keyConds is empty or contains only <> select conditions, scan entire table
    if (keyConds.empty() || keyConds[0].comp == SelCond::NE) {
        //cout << "calling linear scan" << endl;
        linearScan(attr, rf, conds);
        index.close();
		rf.close();
        return 0;
    }
 
/* 
    for (int i = 0; i < keyConds.size(); i++) {
        cout << i << " key ";
        switch (keyConds[i].comp) {
            case SelCond::NE: cout << "!="; break;
            case SelCond::EQ: cout << "=="; break;
            case SelCond::LT: cout << "<"; break;
            case SelCond::LE: cout << "<="; break;
            case SelCond::GT: cout << ">"; break;
            case SelCond::GE: cout << ">="; break;
        }
        cout << " " << keyConds[i].value << endl;
    }
*/    
    // filter keyConds and obtain range of keys to check (if it exists)
    set<IndexEntry> resultsToCheck;
    SelCond lowerBound, upperBound;
    
    //cout << "select: testing1" << endl;
    bool rangeExists = filterConds(keyConds, lowerBound, upperBound);
    //cout << "select: testing2" << endl;
    
/*
    for (int i = 0; i < keyConds.size(); i++) {
        cout << i << " key ";
        switch (keyConds[i].comp) {
            case SelCond::NE: cout << "!="; break;
            case SelCond::EQ: cout << "=="; break;
            case SelCond::LT: cout << "<"; break;
            case SelCond::LE: cout << "<="; break;
            case SelCond::GT: cout << ">"; break;
            case SelCond::GE: cout << ">="; break;
        }
        cout << " " << keyConds[i].value << endl;
    }

*/
    if (rangeExists) {
        //cout << "select: range exists" << endl;
        // if invalid range, return 0 tuples
        // case 1: lower bound > upper bound OR
        // case 2: lower bound < key < lower bound+1    e.g. 14 < key < 15
        if ((atoi(lowerBound.value) >= atoi(upperBound.value)) ||
             (atoi(lowerBound.value)+1 == atoi(upperBound.value) &&
             lowerBound.comp == SelCond::GT &&
             upperBound.comp == SelCond::LT)) {
            //cout << "0 tuples found" << endl;
            index.close();
			rf.close();
            return 0;
        }
        
        // valid range
        else {
            //cout << "select: valid range" << endl;
            //cout << "select: starting getRidsInRange" << endl;
            getRidsInRange(lowerBound, upperBound, index, resultsToCheck);
            //cout << "select: done with getRidsInRange" << endl;
            //cout << "select: # returned keys: " << resultsToCheck.size() << endl; 
        }
    }
    
    else {
        //cout << "select: range doesn't exist" << endl;
        // get keys that satisfy first condition
        //cout << "select: starting getRidsFirstCond" << endl;
        getRidsFirstCond(keyConds[0], index, resultsToCheck);
        //cout << "select: done with getRidsFirstCond" << endl;
        //cout << "select: # returned keys: " << resultsToCheck.size() << endl; 
    
        // if no such keys, return
        if (resultsToCheck.empty()) {
//             cout << "0 tuples found." << endl;
            index.close();
			rf.close();
            return 0;
        }
        
        // filter keys using remaining conditions
        //cout << "select: starting filterKeys" << endl;
        filterKeys(keyConds, resultsToCheck);
        //cout << "select: done with filterKeys" << endl;
        //cout << "select: # returned keys after filter: " << resultsToCheck.size() << endl; 
    }


    // PRINT TUPLES
    // no valueConds to check -- proceed to print tuples
    if (valueConds.empty()) {
        switch (attr) {
    
            case 1:    // print key
                for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++)
                    cout << it->key << endl;
                break;
      
            case 2:    // print value
                for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                    rf.read(it->rid, key, value);
                    cout << value << endl;
                }
                break;
    
            case 3:    // print key and value
                for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                    rf.read(it->rid, key, value);
                    cout << key << " '" << value << "'" << endl;
			}
                break;
      
            case 4:    // print count
                cout << resultsToCheck.size() << endl;
                break;
            }
    }
  
    // there are value conds -- apply them before printing tuples
    else {
        switch (attr) {
    
        case 1:    // print key if value satisfies valueConds
            for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                rf.read(it->rid, key, value);
                if (valueSatisfiesConds(value, valueConds))
                    cout << it->key << endl;
            }
            break;
      
        case 2:    // print value if it satisfies valueConds
            for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                rf.read(it->rid, key, value);
                if (valueSatisfiesConds(value, valueConds))
                    cout << value << endl;
            }
            break;
    
        case 3:    // print key and value if value satisfies valueConds
            for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                rf.read(it->rid, key, value);
                if (valueSatisfiesConds(value, valueConds))
                    cout << key << " '" << value << "'" << endl;
            }
            break;
      
        case 4:    // print count of values that satisfy valueConds
            int count = 0;
            for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                rf.read(it->rid, key, value);
                if (valueSatisfiesConds(value, valueConds))
                    count++;
            }
            cout << count << endl;
            break;
        }
    }
  
  
    // CLEAN UP
    //close the table file and return
    index.close();
	rf.close();
    return 0;
}
    

// if range exists, return true and the SelConds for lower and upper bounds, else false
// note: this function does not check if the range is valid
bool filterConds(vector<SelCond>& conds, SelCond& lowerBound, SelCond& upperBound) {
    vector<SelCond> temp;
    SelCond lowerTemp, upperTemp;
    
    bool rangeExists = false;
    
    //cout << "filterConds: testing1" << endl;
    // check if range exists by attempting to get lower and upper bounds
    bool lowerBoundFound = false;
    bool upperBoundFound = false;

    //cout << "filterConds: testing2" << endl;    
    for (int i = 0; i < conds.size() && (!lowerBoundFound || !upperBoundFound); i++)
        if (!lowerBoundFound && (conds[i].comp == SelCond::GT || conds[i].comp == SelCond::GE)) {
            lowerTemp = conds[i];
            lowerBoundFound = true;
        }
        else if (!upperBoundFound && (conds[i].comp == SelCond::LT || conds[i].comp == SelCond::LE)) {
            upperTemp = conds[i];
            upperBoundFound = true;
        }
    rangeExists = lowerBoundFound && upperBoundFound;
    
    //cout << "filterConds: testing3" << endl;
    // range exists
    if (rangeExists) {
        //cout << "filterConds: testing4" << endl;
        // return lower and upper bounds
        lowerBound = lowerTemp;
        upperBound = upperTemp;
        
        // create new conds vector with only NE and EQ conditions
        for (int i = 0; i < conds.size(); i++)
            if (conds[i].comp == SelCond::NE || conds[i].comp == SelCond::EQ)
                temp.push_back(conds[i]);
    }
    
    // range doesn't exist
    else {
        //cout << "filterConds: testing5" << endl;
        bool lowerBoundAdded = false;
        bool upperBoundAdded = false;
        
        // create new conds vector with only one GT/GE or only one LT/LE
        // note: this code assumes the conds vector is "sorted"
        for (int i = 0; i < conds.size(); i++) {
            switch (conds[i].comp) {
                case SelCond::EQ:  temp.push_back(conds[i]);
                break;
                case SelCond::NE:  temp.push_back(conds[i]);
                break;
                case SelCond::GT:
                case SelCond::GE: 
                    if (!lowerBoundAdded) {
                        temp.push_back(conds[i]);
                        lowerBoundAdded = true;
                    }
                break;
                case SelCond::LT:
                case SelCond::LE:
                    if (!upperBoundAdded) {
                        temp.push_back(conds[i]);
                        upperBoundAdded = true;
                    }
                break;
            }
        }
    }
    
    //cout << "filterConds: testing6" << endl;
    // swap temporary vector with input vector
    conds.swap(temp);
   
/* 
    for (int i = 0; i < conds.size(); i++) {
        cout << i << " key ";
        switch (conds[i].comp) {
            case SelCond::NE: cout << "!="; break;
            case SelCond::EQ: cout << "=="; break;
            case SelCond::LT: cout << "<"; break;
            case SelCond::LE: cout << "<="; break;
            case SelCond::GT: cout << ">"; break;
            case SelCond::GE: cout << ">="; break;
        }
        //cout << " " << conds[i].value << endl;
    }
    
 */   
    // return whether range exists
    return rangeExists;
}

bool valueSatisfiesConds(string& value, const vector<SelCond>& conds) {

    // check the value against every condition
    int diff;
    for (int i = 0; i < conds.size(); i++) {

        // compute the difference between the input value and the condition value
        diff = strcmp(value.c_str(), conds[i].value);

        // return false if any condition is not met
        switch (conds[i].comp) {
            case SelCond::EQ:  if (diff != 0) return false;
            break;
            case SelCond::NE:  if (diff == 0) return false;
            break;
            case SelCond::GT:  if (diff <= 0) return false;
            break;
            case SelCond::LT:  if (diff >= 0) return false;
            break;
            case SelCond::GE:  if (diff < 0)  return false;
            break;
            case SelCond::LE:  if (diff > 0)  return false;
            break;
        }
    }

    // all conditions are met
    return true;
}


//finds all of the rid's in the index satisfying the first condition on keys
void getRidsFirstCond(SelCond condition, BTreeIndex& idx, set<IndexEntry>& resultsToCheck) {    
    IndexCursor cursor;
    RecordId rid;
    int keyInIndex;
	IndexEntry idxEntry;
	bool continueLoop;
	RC error;
    int valueToComp = atoi(condition.value);
	//cout << "value to compare with is " << valueToComp << endl;   
 
    switch(condition.comp) {
      //equality condition - look up directly in index
      case (SelCond::EQ):
        //cout << "checking on equality of key with " << valueToComp << endl;
        if (idx.locate(valueToComp, cursor)!=0) {
          //cout << "on equality check, locate failed to find key greater than or equal to " << valueToComp << endl;
          return;
        }
        if (idx.readForward(cursor, keyInIndex, rid)!=0) {
          //cout << "readForward in check on equality failed" << endl;
          return;
        }
        if (valueToComp == keyInIndex){
          //cout << "found value for key " << valueToComp << " in index, adding rid to resultsToCheck" << endl;
          idxEntry.rid = rid;
		  idxEntry.key = keyInIndex;
		  resultsToCheck.insert(idxEntry);
        }
        else {
          //cout << "in comparing for equality, key not found in index" << endl;
          return;
        }
        return;  
  
      //Greater than or greater than or equal condition
      case (SelCond::GT):
      case (SelCond::GE):
        if (idx.locate(valueToComp, cursor) != 0) {
          //cerr << "no key in index found with key > or >= " << valueToComp << endl;
          return;
        }

		//check first value for GE or GT condition
		error = idx.readForward(cursor, keyInIndex, rid);
		//cout << "first result found is rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;

        if ((condition.comp==SelCond::GE && keyInIndex>=valueToComp) || (condition.comp==SelCond::GT && keyInIndex>valueToComp)){ 
          //cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
          idxEntry.rid = rid;
          idxEntry.key = keyInIndex;
		  resultsToCheck.insert(idxEntry);
        }
        
		if (error !=0 )   //the retrieved key was the last one in the tree - we're done
			return;

		//more to read - read through remaining rid's in index, with keys greater than current location
        continueLoop = true;
		while (continueLoop) {
				error = idx.readForward(cursor, keyInIndex, rid); 
          		//cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
          		idxEntry.rid = rid;
          		idxEntry.key = keyInIndex;
		 	 	resultsToCheck.insert(idxEntry);
				if (error != 0)
					continueLoop = false;
        }
        return;


      //Less than or less than or equal condition
      case (SelCond::LT):
      case (SelCond::LE):
        //cout << "checking on LT or LE condition on key compared to " << valueToComp << endl;
        IndexCursor stoppingCursor;
                  
        //looking for first position in index
        if (idx.locate(0, cursor) != 0) {
          //cerr << "no \"smallest\" key found in index" << endl;
          return;
        }
        //cout << "cursor of start is (pageId: " << cursor.pid << ", eid: " << cursor.eid << ")" << endl;

        //look for stopping position
        if (idx.locate(valueToComp, stoppingCursor) != 0) {
           	//cout << "all keys in index less than " << valueToComp << " - will add all rid's to resultsToCheck" << endl;
            continueLoop = true;
			while (continueLoop) {
				error = idx.readForward(cursor, keyInIndex, rid);
            	//cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
            	idxEntry.rid = rid;
				idxEntry.key = keyInIndex;
				resultsToCheck.insert(idxEntry);
				if (error != 0)
					continueLoop = false;
          	}
			return;
        }

        else {
          //cout << "reading all keys up until stopping cursor: (PageID: " << stoppingCursor.pid << ", eid: " << stoppingCursor.eid << ")" << endl; 
          //read all keys up until stopping point
          IndexCursor nextCursor;
          nextCursor.pid = cursor.pid;
          nextCursor.eid = cursor.eid;
          while (idx.readForward(nextCursor, keyInIndex, rid)==0){
            //cout << "reading forward - reading cursor (pageId: " << cursor.pid << ", eid: " << cursor.eid << ")" << endl;
            if (cursor.pid!=stoppingCursor.pid || cursor.eid!=stoppingCursor.eid) {
              //cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
              idxEntry.rid = rid;
	 		  idxEntry.key = keyInIndex;
			  resultsToCheck.insert(idxEntry);
              cursor.pid = nextCursor.pid;
              cursor.eid = nextCursor.eid;
            }
            else {
              if (condition.comp==SelCond::LE and keyInIndex==valueToComp) {
                //cout << "condition is LE, and key with value " << valueToComp << " is in index - including in resultsToCheck" << endl;
                idxEntry.rid = rid;
				idxEntry.key = keyInIndex;
				resultsToCheck.insert(idxEntry);       
              }
   			  break;
			}
		  }
		return;
	   }

	}  
}

void getRidsInRange(SelCond& lowerBound, SelCond& upperBound, BTreeIndex& idx, set<IndexEntry>& resultsToCheck){
    IndexCursor cursor;
	IndexCursor nextCursor;
    RecordId rid;
    int keyInIndex;
    IndexEntry idxEntry;
    bool continueLoop;
	RC error;
	bool firstIteration;
    int valueToCompLow = atoi(lowerBound.value);
	int valueToCompHigh = atoi(upperBound.value);

    if (TESTING) cout << "getRidsInRange: starting function" << endl;

	//find cursor for lower bound
    if (TESTING) cout << "getRidsInRange: locate on lower bound" << endl;
	error = idx.locate(valueToCompLow, cursor);
	
	//if error - lower bound is too high for index, no tuples to return
	if (error != 0) {
// 		cout << "lower bound is too high, no tuples to return " << endl;
		return;
	}

	//find cursor for upper bound
    if (TESTING) cout << "getRidsInRange: locate on upper bound" << endl;
	IndexCursor stoppingCursor;
	error = idx.locate(valueToCompHigh, stoppingCursor);
	
	//if error - upper bound exceeds values in index - will read forward to the end
	if (error !=0) {
// 		cout << "upper bound exceeds values in index - will read forward to the end" << endl;
		continueLoop = true;
		firstIteration = true;
        if (TESTING) cout << "getRidsInRange: testing 628" << endl;
        while (continueLoop) {
        if (TESTING) cout << "getRidsInRange: testing 630" << endl;
           error = idx.readForward(cursor, keyInIndex, rid);
           //cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
           if (firstIteration) {
        if (TESTING) cout << "getRidsInRange: testing 634" << endl;
                if ((lowerBound.comp==SelCond::GE && keyInIndex>=valueToCompLow) || (lowerBound.comp==SelCond::GT && keyInIndex>valueToCompLow)){
                        if (TESTING) cout << "getRidsInRange: testing 636" << endl;
                    idxEntry.rid = rid;
                    idxEntry.key = keyInIndex;
                    resultsToCheck.insert(idxEntry);            
                }
                firstIteration = false;
           }
           else {
                   if (TESTING) cout << "getRidsInRange: testing 644" << endl;
                idxEntry.rid = rid;
                idxEntry.key = keyInIndex;
                resultsToCheck.insert(idxEntry);
           }
		   if (error !=0)
				continueLoop = false;
		}
		return;
	}

	//otherwise, will read up until stopping cursor
 	else {		
 	    if (TESTING) cout << "getRidsInRange: testing 657" << endl;
		continueLoop = true;
		firstIteration = true;
		nextCursor.eid = cursor.eid;
		nextCursor.pid = cursor.pid;

		//loop through all values - if first iteration, check whether include lower bound
    	while (continueLoop) {
 	    if (TESTING) cout << "getRidsInRange: testing 665" << endl;
	    	//read in value
	    	if (TESTING) cout << "getRidsInRange: starting readForward" << endl;
       		error = idx.readForward(nextCursor, keyInIndex, rid);
	    	if (TESTING) cout << "getRidsInRange: done with readForward" << endl;
 	    if (TESTING) cout << "getRidsInRange: testing 668" << endl;
			//on first iteration, consider whether to include lower bound or not
			if (firstIteration && cursor.pid != stoppingCursor.pid || cursor.eid != stoppingCursor.eid) {
			 	    if (TESTING) cout << "getRidsInRange: testing 671" << endl;
                if ((lowerBound.comp==SelCond::GE && keyInIndex>=valueToCompLow) || (lowerBound.comp==SelCond::GT && keyInIndex>valueToCompLow)){
                    idxEntry.rid = rid;
                    idxEntry.key = keyInIndex;
                    resultsToCheck.insert(idxEntry);
   			 	    if (TESTING) cout << "getRidsInRange: testing 676" << endl;
                }
                firstIteration = false;
				cursor.pid = nextCursor.pid;
				cursor.eid = nextCursor.eid;
           	}
           	else if (cursor.pid!=stoppingCursor.pid || cursor.eid!=stoppingCursor.eid) {
           	   		if (TESTING) cout << "getRidsInRange: testing 681" << endl;
              	//cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
              		idxEntry.rid = rid;
              		idxEntry.key = keyInIndex;
              		resultsToCheck.insert(idxEntry);
              		cursor.pid = nextCursor.pid;
              		cursor.eid = nextCursor.eid;
			}
		
			//have reached stopping condition- check whether to include upper bound
			else {
                if (TESTING) cout << "getRidsInRange: testing 692" << endl;
				if (upperBound.comp==SelCond::LE and keyInIndex==valueToCompHigh) {
                if (TESTING) cout << "getRidsInRange: testing 694" << endl;
                //cout << "condition is LE, and key with value " << valueToComp << " is in index - including in resultsToCheck" << endl;
        	        idxEntry.rid = rid;
            	    idxEntry.key = keyInIndex;
                	resultsToCheck.insert(idxEntry);
				}
				continueLoop = false;
			}
		}
    }
    
    if (TESTING) cout << "getRidsInRange: exiting function" << endl;
}

// filters a given keys with given conditions in place
void filterKeys(const vector<SelCond>& conds, set<IndexEntry>& results)
{
    // return immediately if no conditions to check
    if (conds.empty())
      return;

    int key, diff;
    set<IndexEntry> temp;

    // scan through all IndexEntrys in results
    for (set<IndexEntry>::iterator it = results.begin(); it != results.end(); it++) {  
        key = (*it).key;
    
        // check the key against every condition
        for (int i = 0; i < conds.size(); i++) {
    
            // compute the difference between the key value and the condition value
            diff = key - atoi(conds[i].value);

            // skip the key if any condition is not met
            switch (conds[i].comp) {
                case SelCond::EQ:  if (diff != 0) goto next_key;
                break;
                case SelCond::NE:  if (diff == 0) goto next_key;
                break;
                case SelCond::GT:  if (diff <= 0) goto next_key;
                break;
                case SelCond::LT:  if (diff >= 0) goto next_key;
                break;
                case SelCond::GE:  if (diff < 0)  goto next_key;
                break;
                case SelCond::LE:  if (diff > 0)  goto next_key;
                break;
            }
        }

        // all conditions are met -- add IndexEntry to temp set
        temp.insert(*it);

        // move to the next IndexEntry
        next_key:;
    }
    
    results.swap(temp);
}


RC SqlEngine::linearScan(int attr, RecordFile &rf, const vector<SelCond>& cond)
{
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // scan the table file from the beginning
  rid.pid = rid.sid = 0;
  count = 0;
  while (rid < rf.endRid()) {
    // read the tuple
    if ((rc = rf.read(rid, key, value)) < 0) {
      //fprintf(stderr, "Error: while reading a tuple from table in linearScan\n");
      goto exit_select;
    }

    // check the conditions on the tuple
    for (unsigned i = 0; i < cond.size(); i++) {
      // compute the difference between the tuple value and the condition value
      switch (cond[i].attr) {
      case 1:
	diff = key - atoi(cond[i].value);
	break;
      case 2:
	diff = strcmp(value.c_str(), cond[i].value);
	break;
      }

      // skip the tuple if any condition is not met
      switch (cond[i].comp) {
      case SelCond::EQ:
	if (diff != 0) goto next_tuple;
	break;
      case SelCond::NE:
	if (diff == 0) goto next_tuple;
	break;
      case SelCond::GT:
	if (diff <= 0) goto next_tuple;
	break;
      case SelCond::LT:
	if (diff >= 0) goto next_tuple;
	break;
      case SelCond::GE:
	if (diff < 0) goto next_tuple;
	break;
      case SelCond::LE:
	if (diff > 0) goto next_tuple;
	break;
      }
    }

    // the condition is met for the tuple. 
    // increase matching tuple counter
    count++;

    // print the tuple 
    switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    }

    // move to the next tuple
    next_tuple:
    ++rid;
  }

  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  string tableName = table + ".tbl";
  string indexName = table + ".idx";
  
  //open loadfile as fstream
  ifstream loadStream;
  loadStream.open(loadfile.c_str());
  if (loadStream.fail()) {
    //cerr << "Error opening loadfile." << endl;
    return(RC_FILE_OPEN_FAILED);
  }


  //if table exists but index does not, don't create index
  ifstream table_file(tableName.c_str());
  ifstream index_file(indexName.c_str());
  if (table_file.good() && !index_file.good())
    index = false;


  //open RecordFile - if file does not already exist, is created 
  RecordFile records;
  if (records.open(tableName, 'w')!=0) {
    //cerr << "Error opening record file." << endl;
    return(RC_FILE_OPEN_FAILED);
  }
  
  
  //open index file
  BTreeIndex idx;
  if (index) {
    if (idx.open(indexName, 'w') != 0) {
      //cerr << "Error opening index file." << endl;
      return (RC_FILE_OPEN_FAILED);
    }
  }


  //read in records from loadfile
  string value, recordLine;
  while (getline(loadStream, recordLine)) {
    int key;
    RecordId rid;
    
//    cout << "recordLine is " << recordLine << endl;    

    //parse load line:
    if (parseLoadLine(recordLine, key, value)!=0) {
      //cerr << "Error parsing load line." <<endl ;
      return(RC_INVALID_FILE_FORMAT);
    }  
    
    //store in recordfile:
    if (records.append(key, value, rid)!=0) {
      //cerr<< "Error appending to records file." <<endl;
      return(RC_FILE_WRITE_FAILED);
    }
 
    //store in index if index is selected
    if (index) {
	  //cout << "key being inserted is " << key << endl;
      if (idx.insert(key, rid)!=0) {
        //cerr << "Error inserting value in index in SqlEngine.load()" << endl;
        return(RC_FILE_WRITE_FAILED);
      }
    }  
  //  cout << "Storing line number " <<count << ": key-" << key << " | value-" << value << endl; 
  }
  
  if (index)
    idx.close();
  records.close();
  loadStream.close();
  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
