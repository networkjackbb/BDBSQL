# BDBSQL
BasicDB SQL semi-abstraction library in PHP

Brian Blood
brian@networkjack.info
2022

Not to take away from all the fine work done in many other PHP based database abstraction libraries or PDO, but this library/class has been one of my core pieces of code for almost my entire career working in PHP.


A very long time ago, I was beginning a project that made extensive use of calls to both MySQL and MS SQL Server, often at the same time as it's job was to move data from the latter and put it into the former. As such, I wanted a call interface that was essentially the same for both systems. I also wanted an interface that would focus on a simply way of moving key=>value hashes (one of PHPs strong suits) back and forth into database records. To that end I created a set of standard functions (e.g. DBSelectOne(), DBConnect()) that would act in that fashion, but would hide most, if not all the implementation bits, inside the library. As PHP evolved (and the legacy mysql driver was deprecated), I eventually recreated these functions as three primary and three child classes to cover the same use cases and ones that have come up:

BDB
 - This is the multiton/connection control class. Connections have NAMES that are used as instance keys.

BDB_Base
 - Base class for the engine specific child classes.
 Any logic/functionality that can be handled in a similar fashion across engine with merely a change in PHP function call mysqli_connect vs. mssql_connect is kept here.

BDB_MySQL
 - MySQLi driver specific child class of BDB_Base
 - This child class gets most of the love/attention.
 
BDB_MSSQL
 - Microsoft SQL Server specific child class of BAB
 - The need for this in my own personal/professional space died off many years ago, so it likely does not function, is in dire need of updating to the new sqlsrv driver, and it's main purpose at this time is as a foil to make sure that anything new that might be added overall to this library is done in as neutral way as possible from a driver-vendor perspective.
 - https://www.php.net/manual/en/book.sqlsrv.php


BDBLogging
BDBLogging_ErrorLog
 - These provide a surface area/interface upon which the main classes can rely on for logging purposes.


Terminology Note:

KVHash == array('RecordID' => 4, 'FirstName' => 'John', 'LastName' => 'Doe', 'OrgID' => 42, );
		(One record returned)

KVHashList == array(
				array('RecordID' => 4, 'FirstName' => 'John', 'LastName' => 'Doe', 'OrgID' => 42, ),
				array('RecordID' => 65, 'FirstName' => 'John', 'LastName' => 'Ya-Ya', 'OrgID' => 44, ),
				array('RecordID' => 737, 'FirstName' => 'John', 'LastName' => 'Smallberries', 'OrgID' => 18, ),
		);
		

KeyedKVHashList == array(
			4 => array('FirstName' => 'John', 'LastName' => 'Doe', 'OrgID' => 42, ),
			65 => array('FirstName' => 'John', 'LastName' => 'Ya-Ya', 'OrgID' => 44, ),
			737 => array('FirstName' => 'John', 'LastName' => 'Smallberries', 'OrgID' => 18, ),
		);

KVList == array(	4 => 42, 65 => 44, 737 => 18, );		// RecordID, OrgID



USAGE

The focus of the library is:

	Querying/pulling data from a table/tables, and returning the data as any of the four styles of KVHashes.
	 - SelectOne, SelectList, SelectKeyList
	Insert/Updating one of more records by passing along a KVHash to be updated.
	 - Insert, Replace, Update, UpdateOne
	Deleting one of more records by passing along essentially query parameters.
	 - Delete, DeleteWhere, DeleteEquals

	These and other specific methods are built to handle those main types of operations. They take/return a standardized set of parameters focused around KVHashes.
	 - TableName is almost always the first parameter.
	 - Subsequent parameters can alter behavior/returned data.


	There are other methods that can be called with an actual fully constructed SQL query as the first parameter.
	 - QueryOne, QuerySimpleResult, QueryList
	 - Calling these will usually return a ResultSet for your own iteration/disposal.


A note on string escaping/quoting.
 - Data inside an incoming KVHash for an Insert/Update/Query MUST not be escaped. This is done internally so as to maintain focus on simple in/out transfers ot record data.
 - IF you wanted to pass data in for a column in a KVHash so that the value is NOT escaped, prefix the key/column name with a HASH (#). This will signal to the internal escaping/quoting mechanism to leave the passed value as is. eg.: array('#RecordCreated' => 'NOW()', '#RecCount' => 'ID+1', );
 
 
Query Logging can be turned on:
 - globally
 - through passing a flag in an Options KVHash parameter
 - or by prefixing the tableName parameter with an exclamation point: eg. '!users'

 
Functions that are called for Alerts to query/connection errors can be injected.



 


