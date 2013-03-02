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

//assumes indexfile is already open
void BTreeIndex::printRoot(){
	if (treeHeight == 0) {
		cout << "tree is empty";
		return;
	}

	BTLeafNode * leafNode = new BTLeafNode;
	BTNonLeafNode * nonLeafNode = new BTNonLeafNode;

	if (treeHeight == 1) {  //there is only the root node
		cout << "root is only node in tree:" << endl;
		leafNode->read(rootPid, pf);
		leafNode->printNode();
	}

	else {
		//read in root
		nonLeafNode->read(rootPid, pf);
		cout << "there are " << treeHeight << " levels in tree - root:" << endl;
		nonLeafNode->printAllValues();
	}

	delete leafNode;
	delete nonLeafNode;
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
		
		cout << "new root is now: " << endl;
		newLeaf->printNode();
		
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
			cout << "leaf node is now: " << endl;
			targetLeaf->printNode();

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

			cout << "newly split leaf nodes are: " << endl;
			cout << "on left: " << endl;
			targetLeaf->printNode();
			cout << "on right: " << endl;
			siblingLeaf->printNode();
	
			//delete leaf nodes created
			delete siblingLeaf;
			delete targetLeaf;
			
			//update parent
			//cursor.pid contains pageId of node on left, siblingKey contains value to get pushed up
			//and siblingPid contains pageId of node on right
			cout << "Their parent node (pageId:";
			if (parents.empty())
				cout << "no parent";
			else 
				cout << parents.top();

			cout << ") will now be updated: "<< cursor.pid << " on left, "<< siblingKey << " as key pushed up, and " << siblingPid << "on right" << endl;
			updateParent(cursor.pid, siblingKey, siblingPid);
		}
	}
    return 0;
}

//helper function called to recursively update the BTreeIndex after an insertion at the leaf level, until no splits
RC BTreeIndex::updateParent(PageId right, int key, PageId left) {

	RC error;	

	//check to see if have just split the root node
	if (parents.empty()) {
		cout << "just split root - initializing new root" << endl;

		//create new non-leaf node
		BTNonLeafNode * newRoot = new BTNonLeafNode;
		
		//fill it with the appropriate values
		error = newRoot->initializeRoot(right, key, left);
		if (error != 0) {
			cout << "error initializing new root after insert" << endl;
			return error;
		}

		//write new root to disk
		PageId newRootPid = pf.endPid();
		newRoot->write(newRootPid, pf);

		//change metadata and write to disk
		treeHeight++;
		rootPid = newRootPid;
		writeMetaData();

		printRoot();
		
		delete newRoot;
		return 0;	
	}

	else {
		//get parent node
		PageId parentPid = parents.top();
		parents.pop();

		BTNonLeafNode * parent = new BTNonLeafNode;
		error = parent->read(parentPid, pf);
		if (error != 0) {
			cerr << "error reading in parent node in BTreeIndex updateParent" << endl;
			return error;
		}

		cout << "parent node being updated is: " << endl;
		parent->printAllValues();

		//insert value in parent - if insert works, we're done
		error = parent->insert(key, left);
		if (error = RC_NODE_FULL) {
			cout << "parent with pid " << parentPid << " is full on update after insert in BTreeIndex - splitting" << endl;

			//split node
			int midKey;
			BTNonLeafNode * sibling = new BTNonLeafNode;
			error = parent->insertAndSplit(key, right, *sibling, midKey);
			if (error != 0) {
				cerr << "error in insertAndSplit in updateParents of BTreeIndex" << endl;
				return error;
			}

			//write this new node to disk
			PageId siblingPid = pf.endPid();
			error = sibling->write(siblingPid, pf);
			if (error!=0) {
				cerr << "error writing new sibling to disk after split in updateParents in BTreeIndex" << endl;
				return error;
			}
			
			cout << "node was split into the following nodes:" << endl;
			cout << "on left " << endl;
			parent->printAllValues();
			cout << "on right " << endl;
			sibling->printAllValues();

			cout << "calling update on next parent (PageId ";
			if (parents.empty())
				cout << "no parent";
			else
				cout << parents.top();

			cout<< ") with: "<< parentPid << " on left, "<< midKey << " as key pushed up, and " << siblingPid << "on right" << endl;

			delete sibling;
			delete parent;

			//update parents recursively
			updateParent(parentPid, midKey, siblingPid);
			
			return 0;
		}

		else {
			cout << "update of parents completed successfully at node " << parentPid << " which is now: " << endl;
			parent->printAllValues();	

			//empty the parents stack
			while (!parents.empty()) {
				parents.pop();
			}
			cout << "insertion complete, cleared the parents stack";
		
			delete parent;
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
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor) {
    RC rc;
    PageId pid = rootPid;

    cout << "locate: looking for searchKey " << searchKey << endl;

    // return error if tree height == 0
    if (treeHeight == 0) {
        cout << "error: treeHeight = 0" << endl;
        return RC_NO_SUCH_RECORD;
    }


    // treeHeight has more than one node
    if (treeHeight > 1) {
        cout << "locate: treeHeight > 1, iterating through nodes" << endl;
        BTNonLeafNode nonLeafNode;
        
        // descend tree until leaf node is reached
        int height = 1;
        
        while (height < treeHeight) {
            // read current node into memory
            if ((rc = nonLeafNode.read(pid, pf)) != 0)
                return rc;
            
            // get pointer to child node
            if ((rc = nonLeafNode.locateChildPtr(searchKey, pid)) != 0)
                return rc;
            
            // update current height
            height++;
        }
        cout << "locate: treeHeight > 1, leaf level reached" << endl;
    } // leaf node reached


    // read node into memory
    // (if tree contains only the root node (treeHeight == 1), code starts here)
    cout << "locate: reading leaf node" << endl;
    BTLeafNode leafNode;
    if ((rc = leafNode.read(pid, pf)) != 0)
        return rc;
    
    // find entry for searchKey within leaf node
    cout << "locate: locating searchKey in leaf node" << endl;
    int eid;
    if ((rc = leafNode.locate(searchKey, eid)) != 0) {
        cout << "error: leafNode.locate()" << endl;
        return rc;
    }
    
    // save PageId, entry ID in cursor and return
    cout << "locate: saving pid and eid to cursor" << endl;
    cursor.pid = pid;
    cursor.eid = eid;
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
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid) {
    RC rc;
    BTLeafNode node;
    
    // read the page given by cursor into memory
    cout << "readForward: reading page into memory" << endl;
    if ((rc = node.read(cursor.pid, pf)) != 0)
        return rc;
    
    
    // read entry and store key and rid
    cout << "readForward: reading entry from page" << endl;
    if ((rc = node.readEntry(cursor.eid, key, rid)) != 0)
        return rc;
    
    
    // update cursor
    cout << "readForward: updating cursor to point to next entry" << endl;
    if (cursor.eid == node.getKeyCount() - 1) {
        cout << "readForward: cursor is at last key, setting eid to next node ptr" << endl;
        cursor.eid = (int) node.getNextNodePtr();
    }
    else {
        cout << "readForward: incrementing cursor" << endl;
        cursor.eid++;
    }
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
