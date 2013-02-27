/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <iostream>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{	
	RC error;
	
	//call pf.open()
	error = pf.open(indexname, mode);	
 	if (error != 0)
		return error;
	
	//check if new file 
	if (pf.endPid()==0) {
		cout << indexname << " is empty - initializing rootPid to -1 and treeHeight to 0" << endl;
		rootPid = -1;
		treeHeight = 0;

		//write it immediately to pageId 0 of the file 
		error = writeMetaData();
		if (error != 0)
			return error;
  }

	//else load the meta-info from the first page into member variables rootPid and height
	else {
		char buf[PageFile::PAGE_SIZE];
    	int *ptr = (int*)buf;
		
		error = pf.read(0,buf);
		if (error != 0)
			return error;
		rootPid = *ptr;
		ptr++;
		treeHeight = *ptr;
	}	

	cout << "info loaded from file " << indexname << ": rootPid is "<< rootPid << " and treeHeight is " << treeHeight << endl; 

    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	return (pf.close());	
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	RC error;
	
	//if tree is empty 
	if (treeHeight == 0) {
		cout<< "in insert: tree is empty, creating new root node" << endl;
	
		//create an empty leafNode 
		BTLeafNode* newLeaf = new BTLeafNode;

		//insert into new node
		cout << "inserting in newly created root" << endl;
		error = newLeaf->insert(key, rid);
		if (error != 0) {
			cerr << "error inserting in newly created root node in BTreeIndex insert" << endl;
			return error;
		}

		//set nextNodePtr 
		newLeaf->setNextNodePtr(-1);

		//set metadata of index to reflect changes
		treeHeight = 1;  
		rootPid = pf.endPid();
		cout << "new root is getting assigned pageId: " << pf.endPid() << endl;
	
		//write it all back to disk: 
		error = writeMetaData();
		if (error != 0) {
			cerr << "problem writing metaData to disk when creating new root in insert" << endl;
			return error;
		}
		error = newLeaf->write(pf.endPid(), pf);
		if (error != 0) {
			cerr << "error writing initialized root node to disk in BTreeIndex insert" << endl;
			return error;
		}
		
		delete newLeaf;
		return 0;
	}

	else {	//tree is not empty
		IndexCursor cursor;
		error = locate(key, cursor);  //will keep track of path followed in vector "parents"
		if (error != 0) {
			cerr << "error in call to locate in BTreeIndex insert" << endl;
			return error;
		}

		//create new BTLeafNode 
		BTLeafNode * targetLeaf = new BTLeafNode;

		//read the contents 
		error = targetLeaf->read(cursor.pid, pf);
		if (error != 0) {
			cerr << "error reading from leafNode in BTreeIndex insert" << endl;
			return error;
		}
		
		//try to insert in node
		error = targetLeaf->insert(key, rid);  
		
		//if node not full - no need to update parent or metadata - write leaf node to disk and return 
		if (error != RC_NODE_FULL) {
			cout << "successfully inserted in leaf node - no update of parent necessary" << endl;
			error = targetLeaf->write(cursor.pid, pf);
			if (error != 0) {
				cerr << "error writing updated leafNode in BTreeIndex when no parent update necessary" << endl;
				return error;
			}
			delete targetLeaf;
			return 0;
		}

		else {     //need to create sibling node
			//create new leaf node 
			BTLeafNode * siblingLeaf = new BTLeafNode;

			int siblingKey;  //will be filled by call to insertAndSplit with new key for parent
			int siblingPid = pf.endPid();  //set pid for new sibling node
			int siblingNodePtr = targetLeaf->getNextNodePtr();
			error = targetLeaf->insertAndSplit(key, rid, *siblingLeaf, siblingKey);
			if (error != 0) {
				cerr << "error calling leafNode insertAndSplit from BTreeIndex insert" << endl;
				return error;
			}			

			//reset node pointers:
			siblingLeaf->setNextNodePtr(siblingNodePtr);
			targetLeaf->setNextNodePtr(siblingPid);

			//write to disk
			error = siblingLeaf->write(siblingPid, pf);
			if (error != 0) {
				cerr << "error writing newly created sibling node at leaf level insert in BTreeIndex insert" << endl;
				return error;
			}
			
			error = targetLeaf->write(cursor.pid, pf);
			if (error != 0) {
				cerr << "error writing targetLeaf to disk in BTreeIndex insert after split of leaf node" << endl;
				return error;
			}

			//delete leaf nodes created
			delete siblingLeaf;
			delete targetLeaf;
			
			//update parent with value in siblingKey
			//
			//
			//
			//
			//
			//
			//
			//
			//
			//
		}
	}
    return 0;
}

/*
 * Find the leaf-node index entry whose key value is larger than or 
 * equal to searchKey, and output the location of the entry in IndexCursor.
 * IndexCursor is a "pointer" to a B+tree leaf-node entry consisting of
 * the PageId of the node and the SlotID of the index entry.
 * Note that, for range queries, we need to scan the B+tree leaf nodes.
 * For example, if the query is "key > 1000", we should scan the leaf
 * nodes starting with the key value 1000. For this reason,
 * it is better to return the location of the leaf node entry 
 * for a given searchKey, instead of returning the RecordId
 * associated with the searchKey directly.
 * Once the location of the index entry is identified and returned 
 * from this function, you should call readForward() to retrieve the
 * actual (key, rid) pair from the index.
 * @param key[IN] the key to find.
 * @param cursor[OUT] the cursor pointing to the first index entry
 *                    with the key value.
 * @return error code. 0 if no error.
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    return 0;
}


//--------------------------------helper functions------------------------------

RC BTreeIndex::writeMetaData() {
	RC error;
    char buf[PageFile::PAGE_SIZE];
    int *ptr = (int*)buf;

    *ptr = rootPid;
    ptr++;
	*ptr = treeHeight;
    error=pf.write(0, buf);
    if (error!=0)
		return error;

	return 0;
}
