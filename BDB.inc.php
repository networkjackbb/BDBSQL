<?php

/*
BDB vers: 1.9.7

This class is meant to provide easy access to instantiated BDB database objects
It does this by allowing you to declare a new child class of BDB with any aribtrary name (usually something like "PrimaryDB")

		Example usage
class ProjectDB extends BDB {}

ProjectDB::Make('MySQL', 'PrimaryDB', $CnxnParams);

BDB::Make('MySQL', 'PrimaryDB', $CnxnParams);
ProjectDB::Claim('PrimaryDB');

$DB = ProjectDB::Get('PrimaryDB');
$User1Row = $DB->SelectOne('users', '', 'id = 1');

$DelResult = $DB->DeleteEquals('domains', 'id', 443);


TODO
- Allow more values to be passed in that are pushed in through mysqli->options();
- add method to get current version of particular engines library.
- Event Logging - connect, query, disconnect, etc....
- DupKeyUpdate - allow passing comma separated field list - fxn will do the setup.
- Finish Upsert
- BDB_DataObject
- BDB_Record $this->Record->select($fields)->from("module_rows")->where("module_id", "=", $module_id);
*/

class BDB
{
	const kSQLDATETIMEFMT = 'Y-m-d H:i:s';

	protected static $MyDB = false;
	private static $EngineTypes = array();
	private static $TypeEngines = array();
	private static $BDBs = array();

### -------------------------------------------------------------------------
## don't allow instantiation of this class
	private function __construct() {}

	
	public static function Get($inName=false)
	{
		if (false === $inName)			return self::$MyDB;
		$nameKey = strtolower($inName);
		if (!array_key_exists($nameKey, self::$BDBs))			return false;
		return self::$BDBs[$nameKey];
	}

	public static function DB($name)
	{
		return self::Get($name);
	}

	private static function Set($inName, $BDB_Object)
	{
		$nameKey = strtolower($inName);
		self::$BDBs[$nameKey] = $BDB_Object;
	}

	public static function __callStatic($func, $Args)
	{
		if (false === static::$MyDB)				throw new Exception('No DB Defined');
		if ('BDB' === get_called_class())				throw new Exception('BDB not extended');
		return call_user_func_array(array(static::$MyDB, $func), $Args);
	}
/*
hack for the pass by reference issue:
   function executeHook($name, $type='hooks'){ 
        $args = func_get_args(); 
        array_shift($args); 
        array_shift($args); 
        //Rather stupid Hack for the call_user_func_array(); 
        $Args = array(); 
        foreach($args as $k => &$arg){ 
            $Args[$k] = &$arg; 
        } 
        //End Hack 
        $hooks = &$this->$type; 
        if(!isset($hooks[$name])) return false; 
        $hook = $hooks[$name]; 
        call_user_func_array($hook, $Args); 
    } 
*/


## returns true or a text error.
## noops if there is already a BDB registered with the same name.
	public static function Make($inType, $name, $CnxnParams, $Services=array())
	{
		$type = strtolower($inType);
		if (!array_key_exists($type, self::$TypeEngines))				return "Unknown Engine Type: $inType";
		if (false === self::Get($name))
		{
			$bdbClass = 'BDB_'. (self::$EngineTypes[self::$TypeEngines[$type]]);			//	BDB_MySQL
			$NewBDB = new $bdbClass($name, $CnxnParams, $Services);
			if ('BDB' != get_called_class())			self::$MyDB = $NewBDB;
			self::Set($name, $NewBDB);
		}
		return true;
	}

	public static function Claim($name)
	{
		if ('BDB' == get_called_class())		return false;
		$BDB = self::Get($name);
		if (false === $BDB)			return false;
		self::$MyDB = $BDB;
		return true;
	}

	public static function RegisterEngineType($typeID, $typeName)
	{
		self::$EngineTypes[$typeID] = $typeName;
		self::$TypeEngines[strtolower($typeName)] = $typeID;
	}


	public static function DBList()
	{
		$DBList = array();
		foreach(self::$BDBs as $nameKey => $DB)
		{
			$DBList[$nameKey] = $DB->GetInfo();
		}
		return $DBList;
	}

	public static function DisconnectAll()
	{
		foreach(self::$BDBs as $DB)
		{
			if ($DB->IsConnected())				$DB->Disconnect();
		}
	}

}		//	BDB

##	class that is meant to be extended by db engine specific code

## Create some methods that deal with asking about the result returned from a db operation
##		$DBResult


class BDB_Base
{
	const kEngineTypeID = 0;
	const kEngineTypeName = 'BDB';

	const kSQLDATETIMEFMT = BDB::kSQLDATETIMEFMT;

	protected $DBParams = array();

	public $cnxnName = null;
	protected $cnxnRef = null;		// cnxn id rsrc or open file pointer or driver object
	protected $CnxnOpts = array();

	protected $connStartTime = 0;
	protected $numQueries = 0;

	public $logQueries = false;
	protected $QueryLogger = false;
	protected $logThisQuery = false;
	protected $qLogFileTgt = false;
	protected $QueryLog = array();

	public $logQueryErrors = true;
	protected $QueryErrorAlerter = false;
	protected $qErrLogFileTgt = false;
	protected $QErrData = array();

	protected $ErrorInfo = array();
	
	protected $lastQuery = '';
	protected $lastQueryTgtTable = '';
	protected $lastQueryTime = 0;
	
	protected $Services = array();

	protected $defaultBoolTest = 'AND';

	protected static $fxnNameDisconnect = false;
	protected static $fxnNameEscapeString = false;
	protected static $fxnNameFreeResult = false;
	protected static $fxnNameQuery = 'null_op';



	public function __construct($name=false, $CnxnParams, $Services=array())
	{
		$this->cnxnName = $name;
		$this->SetCnxnParams($CnxnParams);

		if (!empty($Services))
		{
			$this->Services = array_merge($this->Services, $Services);
		}

#		if (array_key_exists('QueryLogger', $Services))
#		{
#			$this->SetQueryLogger($Services['QueryLogger']);
#		}
#
		if (array_key_exists('QueryErrorAlerter', $Services))
		{
			$this->SetQueryErrorAlerter($Services['QueryErrorAlerter']);
		}
	}		//	__construct
	
	protected final function null_op()		{}

	protected function SetCnxnParams($CnxnParams)
	{
		foreach($CnxnParams as $key => $val)
		{
			switch(strtolower($key))
			{
				case 'logqueries':
					$this->logQueries = $this->logThisQuery = (bool)$val;
					break;
				case 'logerrors':
					$this->logQueryErrors = (bool)$val;
					break;
				case 'errorlogging':
				case 'erroralerter':
				case 'queryerroralerter':
					$result = $this->SetQueryErrorAlerter($val);
					if (true !== $result)		error_log("attempting to set error log in CnxnParams failed: $result\n". var_export($val, true));
					break;
			}
		}
		return $this;
	}



	public function GetDBParams()		{}
	public function SetDBParams($DBParams)		{}
	public function Connect()		{}


	public function Disconnect()
	{		// simple version of Disconnect.
		$cnxnRef = $this->cnxnRef;
		$this->cnxnRef = null;
		if (is_null($cnxnRef))			return false;
		$fxn = static::$fxnNameDisconnect;
		return $fxn($cnxnRef);
	}

	public function IsConnected()
	{
		return (!is_null($this->cnxnRef));
	}

	public function EscapeString($data)		{		return $data; }

	public function Quote($data)
	{
		if (!is_array($data))
		{
			$vtype = gettype($data);
			if ( ('integer' == $vtype) || ('double' == $vtype) )		return $data;
			return "'". $this->EscapeString($data) ."'";
		}

		$NewData = array();
		foreach(array_keys($data) as $k)
		{
			$v = $data[$k];
			$vtype = gettype($v);
			if ( ('integer' == $vtype) || ('double' == $vtype) )
				$NewData[$k] = $v;
			else	$NewData[$k] = "'". $this->EscapeString($v) ."'";
		}
		return $NewData;
	}






	public function Ping()		{	return false; }
	public function Epoch()		{}
	public function IsEpoch()		{}
	public function AsSQLDT($time=false)
	{		 ## 	formats a datetime as a UTC/GM time according to the engines preferred datetime format
		if (false !== $time)
		{
			$timeInt = preg_replace('/[^0-9]/', '', $time);
			$timeLen = strlen($time);
			if (strlen($timeInt) == $timeLen)
			{			// we've been passed a timestamp - 20010301090807 or 1029733200
				if (14 == $timeLen)			$time = gmmktime(substr($time,8,2), substr($time,10,2), substr($time,12,2), substr($time,4,2), substr($time,6,2), substr($time,0,4));
			}
			else
			{
				$time = strtotime($time);		// this can be a relative time (3 weeks ago)
				if (false === $time)		return false;
			}
		}	else		$time = time();
		return gmdate($this::kSQLDATETIMEFMT, $time);
	}

	public function GetInfo($DBInfo=array())
	{
		$DBInfo['type'] = $this::kEngineTypeName;
		$DBInfo['cnxn_name'] = $this->cnxnName;
		$DBInfo['host'] = $this->Host();
		$DBInfo['username'] = $this->Username();
		$DBInfo['cnxn_started'] = $this->Stats('cnxn_started');
		$DBInfo['num_queries'] = $this->Stats('num_queries');
		$DBInfo['connected'] = $this->IsConnected();
		return $DBInfo;
	}


	public function Host()		{}
	public function Username()		{}
	public function Stats($stat)
	{
		switch(strtolower($stat))
		{
			case 'cnxn_started':		return $this->connStartTime;
			case 'num_queries':		return $this->numQueries;
		}
	}


// tableName/queryPart is passed by reference so it can be corrected
	protected function InitQuery(&$queryPart, $Options=array())
	{
		$this->QueryOptions = $Options;		// save these for others to have access to.

		
		$logThisQuery = (substr($queryPart, 0,1) == '!');
		if ($logThisQuery)
		{
			$queryPart = substr($queryPart, 1);
		}
		else
		{
			## This could theoretically turn OFF logging for this single query, even if logging were enabled globally. This is a feature, not a bug.
			$logThisQuery = array_key_exists('logquery', $this->QueryOptions) ? boolval($this->QueryOptions['logquery']) : $this->logQueries;
		}
		$this->logThisQuery = $logThisQuery;


		$this->lastQuery = $this->lastQueryTgtTable = '';

		return $this;
	}


	public function LastQuery()					{		return $this->lastQuery;		}
	public function LastQueryTime()			{		return $this->lastQueryTime;		}
	


## these need to be overridden by subclass
	public function SelectOne($table, $colList, $where='')		{	$this->InitQuery($table);	return array();	}
	public function SelectCell($query)			{	$this->InitQuery($query);	return;	}

## shorthand versions
	public function SelectKeyList($tableName, $keyField, $where, $orderBy='', $limitInput=0)
	{
		return $this->SelectList($tableName, '', $keyField, '', $where, $orderBy, $limitInput);
	}


## ---------- Query, return result set pointer -------------

## Direct Queries can be directed to log by prefixed a '!' to the query string.
	public function Query($query, $Options=array())
	{
		$this->InitQuery($query, $Options);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');

		$this->lastQuery = $query;
		if ($this->logThisQuery)					$this->LogQuery(__function__);
		$this->numQueries++;
		$fxn = static::$fxnNameResultsQuery;
#		try {#		}	catch
		$qtime = -microtime(true);
		$QR = $fxn($this->lastQuery, $this->cnxnRef);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false !== $QR)		return $QR;

		return $this->HandleQueryError(__function__, array('called_fxn' => $fxn));
	}

	public function DQuery($query, $Options=array())
	{		// Direct Query, no fluff
		$this->InitQuery($query, $Options);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');

		$this->lastQuery = $query;
		if ($this->logThisQuery)					$this->LogQuery(__function__);
		$this->numQueries++;
		$fxn = static::$fxnNameQuery;
		$qtime = -microtime(true);
		$QR = $fxn($this->cnxnRef, $query);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false !== $QR)		return $QR;

		return $this->HandleQueryError(__function__, array('called_fxn' => $fxn));
	}


## This should return either a data value or false
	public function QuerySimpleResult($queryString)			{}

	public function SelectList($tableName, $colList, $keyField, $nameField, $where, $orderBy='', $limitInput=0)		{	return array(); }
	public function QueryOne($sqlQuery)		{	return array(); }
	public function QueryList($sqlQuery, $keyField='', $nameField='')		{	return array(); }
	public function Insert($tableName, $DataRecord, $Options=array())		{}
	public function Replace($tableName, $DataRecord, $Options=array())		{}
	public function Update($tableName, $DataRecord, $where, $Options=array())		{}
	public function UpdateOne($tableName, $DataRecord, $where, $Options=array())
	{
		$Options['limit'] = 1;
		return $this->Update($tableName, $DataRecord, $where, $Options);
	}

	public function Delete($table, $where)		{}
	public function DeleteOne($table, $where)		{}
	public function SetTransIsolationLevel()		{}
	public function FetchAssoc($QR)
	{
		$fxn = static::$fxnNameFetchAssoc;
		return $fxn($QR);
	}
	public function FetchRow($QR)		{	return array();	}
	public function LastInsertID($param1)		{		return 0;	}



## ---------- Query Result/Set Processing -------------

## return format for FetchResultSet is dependent on the keyfield/namefield parameters.
	public function FetchResultSet($QR, $keyField='', $nameField='')
	{
		$numRows = $this->QueryNumRows($QR);
		$ResultData = array();
		while ($numRows)
		{
			$numRows--;
			$RowData = $this->FetchRow($QR);
			if (!empty($keyField))
			{
				if (empty($nameField))
				{
					$theKey = $RowData[$keyField];
					unset($RowData[$keyField]);
					$ResultData[$theKey] = $RowData;
				}
				elseif ('*' == $nameField)
				{
					$ResultData[$RowData[$keyField]] = $RowData;
				}
				else
				{
					$ResultData[$RowData[$keyField]] = $RowData[$nameField];
				}
			}
			else
			{
				$ResultData[] = empty($nameField) ? $RowData : $RowData[$nameField];
			}
		}
		return $ResultData;
	}


	public function DisposeQuery($QR)
	{
		if ( !(is_object($QR) || is_resource($QR) ))		return false;
		$fxn = static::$fxnNameFreeResult;
		return $fxn($QR);
	}

	public function QueryNumRows($QR)
	{
		if ( !(is_object($QR) || is_resource($QR) ))		return 0;
		$fxn = static::$fxnNameNumRows;
		return $fxn($QR);
	}

	public function NumAffectedRows($QR)
	{
		if (is_null($this->cnxnRef))		return false;
		$fxn = static::$fxnNameNumAffectedRows;
		return $fxn($QR, $this->cnxnRef);
	}


## ---------- Transactions -------------

	public function BeginTransaction()
	{
		return $this->DQuery(static::$sqlBeginTrxn);
	}
	public function Begin()				{			return $this->BeginTransaction(); }

	public function CommitTransaction()
	{
		return $this->DQuery(static::$sqlCommitTrxn);
	}
	public function Commit()				{			return $this->CommitTransaction(); }
	
	public function RollbackTransaction()
	{
		return $this->DQuery(static::$sqlRollbackTrxn);
	}
	public function Rollback()				{			return $this->RollbackTransaction(); }




## Utility functions


	public function BuildWhere($TheData)
	{
		if (empty($TheData))			return '';
		if (!is_array($TheData))			return $TheData;
		if ('AND' == $this->defaultBoolTest)			return $this->PrepareColDataAND($TheData);
		return $this->PrepareColDataOR($TheData);
	}



## 	array('Col1'=>'A', 'Col1'=>'A', '#Col3'=>'NOW()', 'Col4|!='=>'3333', '#Col5|>'=>'NOW()', )
## Result: Col1='A', Col2='B', Col3=NOW(), Col4!='3333', Col5 > NOW()
	public function PrepareColData($TheData)
	{
		$Result = array();

		if (!is_array($TheData))	return $Result;

		$LiteralKeys = array();		// used to resolve multiple passed fields down to one and prefer "#" passed fields in the process
		foreach(array_keys($TheData) as $dataKey)
		{
			$normalKey = strtolower($dataKey);
			if (array_key_exists($normalKey, $LiteralKeys))		continue;
				// if we've seen this key as a literal key before, do not override with this new non-literal version
				
			$colName = $dataKey;

			$useQuotes = true;		//	!is_numeric($theData);
			if ('#' == substr($normalKey, 0, 1))
			{		// the key/value passed is to built as literal....
				$normalKey = substr($normalKey, 1);
				$colName = substr($colName, 1);
				$useQuotes = false;
				$LiteralKeys[$normalKey] = $colName;
			}

			if (NULL === $TheData[$dataKey])
			{
				$Result[$normalKey] = $colName .' = NU'.'LL';
				continue;
			}

			$op = '=';
			if (strstr($colName, '|'))
			{
				list($colName, $op) = explode('|', $colName, 2);
			}
			
			$Result[$normalKey] = "$colName $op ". ($useQuotes ? $this->Quote($TheData[$dataKey]) : $TheData[$dataKey]);
		}
		
		return $Result;
	}		//	PrepareColData

## 	array('Col1'=>'A', 'Col1'=>'A', '#Col3'=>'NOW()', 'Col4|!='=>'3333', '#Col5|>'=>'NOW()', )
## Result: Col1='A', Col2='B', Col3=NOW(), Col4!='3333', Col5 > NOW()
	protected function PrepareColDataList($TheData)
	{
		if (!is_array($TheData) || (count($TheData) == 0))	return false;
		return implode(',  ', $this->PrepareColData($TheData));
	}

## Result: (Col1='A') OR (Col2='B')
	public function PrepareColDataOR($TheData)
	{
		if (!is_array($TheData) || (count($TheData) == 0))	return false;
		return '('. implode(') OR (', $this->PrepareColData($TheData)) .')';
	}

## Result: (Col1='A') AND (Col2='B')
	public function PrepareColDataAND($TheData)
	{
		if (!is_array($TheData) || (count($TheData) == 0))	return false;
		return '('. implode(') AND (', $this->PrepareColData($TheData)) .')';
	}


## -------- Query Logging --------------

	### $this->QueryLogger can be one of three values:
	###			boolean = false => do nothing
	###			an object => the LogQuery method of that object is called
	###			a callable => the fxn is called. 
	protected function LogQuery($fxn, $query=false)
	{		 // Explicitly log this query as it will only be called if {logQueries} is set to true

		$QLogData = array('fxn' => $fxn, );
		$QLogData['query'] = (false === $query) ? $this->lastQuery : $query;
		$QLogData['qtime'] = $this->lastQueryTime;
		$QLogData['when_ut'] = microtime(true);
		$LoggingEventDT = new DateTime();
		$QLogData['when_fmt'] = $LoggingEventDT->format('Y-m-d H:i:s'); ## .v');		//	Y-m-d\TH:i:s.v
		$QLogData['cnxn'] = self::kEngineTypeID .':'. $this->cnxnName;
	
		if (false !== $this->QueryLogger)
		{
			if (is_object($this->QueryLogger))
				{	$this->QueryLogger->LogQuery($QLogData);	}
			else
				{	call_user_func($this->QueryLogger, $QLogData);	}
		}
		return $this;
	}


## returns either true or text error message.
	public function SetQueryLogger($qryLgr, $tgt=false)
	{
		if (is_object($qryLgr))
		{
			if (!is_callable(array($qryLgr, 'LogQuery')))			return get_class($qryLgr) .'::LogQuery() is not a callable method';
			$this->QueryLogger = $qryLgr;
		}
		elseif ( ('error_log' === $qryLgr) || ('error_log_file' === $qryLgr) )
		{
			$this->QueryLogger = array($this, 'QueryLogger_ErrorLog');
			if ( ('error_log_file' === $qryLgr) && (false !== $tgt) )
			{
				if (!file_exists($tgt))		@touch($tgt);		## try and create the file.
				if (!file_exists($tgt))			return "Unable to create query log output file: $tgt";
				if (!is_writable($tgt))			return "Query log output file not writable: $tgt";
				$this->qLogFileTgt = $tgt;
			}
			else $this->qLogFileTgt = false;

		}
		elseif ( (true === $qryLgr) || ('memory' === $qryLgr) )
		{
			$this->QueryLogger = array($this, 'QueryLogger_Memory');
		}
		elseif (is_callable($qryLgr))
		{
			$this->QueryLogger = $qryLgr;
		}
		else		return "Unable to Set QueryLogger to: ". var_export($qryLgr, true);
		
		return true;
	}

	protected function QueryLogger_ErrorLog($QLogData)
	{
		$fxn = $QLogData['fxn'];
		$query = $QLogData['query'];
		if (false === $this->qLogFileTgt)
			error_log("$fxn - $query");
		else error_log("$fxn - $query\n", 3, $this->qLogFileTgt);
		return $this;
	}

	protected function QueryLogger_Memory($QLogData)
	{
		$fxn = $QLogData['fxn'];
		$query = $QLogData['query'];
		$eventDate = $QLogData['when_fmt'];
		$this->QueryLog[] = $eventDate ."\t". $fxn ."\t". $query;
		return $this;
	}
	

## TODO - max returned.
	public function GetQueryLog($max=false)
	{
		return $this->QueryLog;
	}


## -------- Query Error Handling --------------

## Called when the lowest level connection object query method returns a "false"
## will always return a "false"
	protected function HandleQueryError($fxn, $AddlData=array())
	{
		$this->QErrData = array('fxn' => $fxn, );
		$this->QErrData['cnxn'] = $this::kEngineTypeName .':'. $this->cnxnName;
		$this->QErrData['database'] = $this->defaultDB;
		$this->QErrData['target_table'] = $this->lastQueryTgtTable;
		$this->QErrData['when_ut'] = microtime(true);
		$LoggingEventDT = new DateTime();
		$this->QErrData['code'] = $this->ErrorNum();
		$this->QErrData['text'] = $this->ErrorStr();
		if (!empty($this->QueryOptions))			$this->QErrData['query_options'] = $this->QueryOptions;
		$this->QErrData['backtrace'] = $this->GetQueryBT();
		$this->QErrData['query'] = $this->lastQuery;

		$this->QErrData = array_merge($this->QErrData, $AddlData);

		return $this->QueryErrorAlert();
	}

## for non-select ops that expect a result block back with a status flag
	protected function HandleQueryErrorResult($fxn, $AddlData=array())
	{
		$Result = array('Status' => false, );
		$Result += $this->HandleQueryError($fxn, $AddlData);
		return $Result;
	}

## Called when there is something wrong with the parameters passed that WILL result in a query error.
## the query was not constructed, so we blank it out then pass on to the normal chain for query error processing/alerting.
	protected final function HandleQueryParamsError($fxn, $msg)
	{
		$this->QErrData = array('fxn' => $fxn, );
		$this->QErrData['msg'] = $msg;
		$this->QErrData['query'] = $this->lastQuery;
		$this->QErrData['backtrace'] = $this->GetQueryBT();		
		return $this->QueryErrorAlert();
	}

## for non-select ops that expect a result block back with a status flag
	protected function HandleQueryParamsErrorResult($fxn, $msg)
	{
		$Result = array('Status' => false, 'ErrorNum' => -1, 'ErrorStr' => $msg);
		$Result['err_handled'] = $this->HandleQueryParamsError($fxn, $msg);
		return $Result;
	}


## A standalone function for handling whatever alerting/logging/{messenger pigeon} is configured when a query error occurs.
	protected final function QueryErrorAlert()
	{
		$logThisError = array_key_exists('logerrors', $this->QueryOptions) ? boolval($this->QueryOptions['logerrors']) : $this->logQueryErrors;
		## the caller can explicitly request to NOT have an error alert raised if this specific query results in an error.
		if (!$logThisError)			return;

			## default value
		if (true === $this->QueryErrorAlerter)			return $this->QueryErrorAlerter_ErrorLog();
		
		if (false !== $this->QueryErrorAlerter)
		{
			if (is_object($this->QueryErrorAlerter))
				{	$this->QueryErrorAlerter->LogQError($this->QErrData);	}
			else
				{	call_user_func($this->QueryErrorAlerter, $this->QErrData);	}
		}
		return false;
	}


## returns either true or text error message.
	public function SetQueryErrorAlerter($qryErrLgr, $tgt=false)
	{
		$this->logQueryErrors = true;
		if ( is_array($qryErrLgr) && (1 == count($qryErrLgr)) )
		{		## array('error_log_file'=> 'file')
			$tgt = current($qryErrLgr);
			$qryErrLgr = key($qryErrLgr);
		}

		if (is_object($qryErrLgr))
		{
			if (!is_callable(array($qryErrLgr, 'LogQuery')))			return get_class($qryErrLgr) .'::LogQuery() is not a callable method';
			$this->QueryErrorAlerter = $qryErrLgr;
		}
		elseif ( ('error_log' === $qryErrLgr) || ('error_log_file' === $qryErrLgr) )
		{
			$this->QueryErrorAlerter = true;
			$this->qErrLogFileTgt = false;		// set to default of php error log as a contigency.
			if ( ('error_log_file' === $qryErrLgr) && (false !== $tgt) )
			{
				if (!file_exists($tgt))		@touch($tgt);		## try and create the file.
				if (!file_exists($tgt))			return "Unable to create query log output file: $tgt";
				if (!is_writable($tgt))			return "Query log output file not writable: $tgt";
				$this->qErrLogFileTgt = $tgt;		 // Set this variable regardless of the outcomes below.
			}
		}
		elseif (is_callable($qryErrLgr))
		{
			$this->QueryErrorAlerter = $qryErrLgr;
		}
		elseif (false === $qryErrLgr)
		{
			$this->QueryErrorAlerter = false;
			$this->logQueryErrors = false;
		}
		else
		{
			$this->logQueryErrors = false;
			return "Unable to Set QueryErrorAlerter to: ". var_export($qryErrLgr, true);
		}
		
		return true;
	}		//	SetQueryErrorAlerter


	protected function QueryErrorAlerter_ErrorLog()
	{
		$fxn = $this->QErrData['fxn'];
		$cnxn = $this->QErrData['cnxn'];
		$dbName = $this->defaultDB;
		$tableName = $this->lastQueryTgtTable;
		$dbErr = $this->QErrData['code'] .':'. $this->QErrData['text'];
		$errMsg = "DBCnxn: $cnxn: $fxn Execution has failed on $dbName" . (empty($tableName) ? '' : '.'.$tableName) ."\n";
		$errMsg .= "Error:$dbErr\nQuery:". $this->lastQuery ."\n". $this->QErrData['backtrace'];

		if (false === $this->qErrLogFileTgt)
			error_log($errMsg);
		else
		{
			$when = DateTime::createFromFormat('U.u', $this->QErrData['when_ut'])->format('Y-m-d h:i:s.v');
			error_log($when .' '. $errMsg ."\n", 3, $this->qErrLogFileTgt);
		}
		return false;
	}


## For errors related to events NOT query related
## when detected, caller can retrieve info by called GetDBError()
	protected final function DBError($fxn, $msg=false)
	{
		$this->ErrorInfo['fxn'] = $fxn;
		$this->ErrorInfo['msg'] = (false === $msg) ? '' : $msg;
		return false;	
	}

## Same as above but puts something into the PHP errorlog
	protected final function DBErrorAlert($fxn, $msg=false)
	{
		error_log("$fxn" . ((false === $msg) ? '' : " - $msg") );
		return $this->DBError($fxn, $msg);
	}



	protected function GetQueryBT()
	{
		$FullBacktrace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS);
		
		## We want to not spam this BT output with a bunch of irrelevant lines, so strip
		##		all but the last method where the class is from this family.
		$SlimBacktrace = array();
		$curClassName = get_class($this);
		foreach($FullBacktrace as $stepNum => $StepInfo)
		{
			if (array_key_exists('class', $StepInfo))
			{
				$class = $StepInfo['class'];
				if ( ('BDB_Base' == $class) || ($class == $curClassName) )
				{
					$SlimBacktrace = array($StepInfo);
					continue;
				}
			}
			$SlimBacktrace[] = $StepInfo;
		}

		$DebugOut = array();
		$SlimBacktrace = array_reverse($SlimBacktrace, false);
		foreach($SlimBacktrace as $stepNum => $StepInfo)
		{
			$stepLine = "$stepNum.\t";
			if (array_key_exists('class', $StepInfo))
			{
				$stepLine .= $StepInfo['class'] . $StepInfo['type'];
			}
			$stepLine .= $StepInfo['function'] .'::'. $StepInfo['line'] ."\t". $StepInfo['file'];
			$DebugOut[] = $stepLine;
		}
		return implode("\n", $DebugOut);
	}


	protected function QErrResultInit()
	{
		$Result = array('Status' => false, 'Query' => $this->lastQuery, );
		$Result['ErrorNum'] = $this->ErrorNum();
		$Result['ErrorStr'] = $this->ErrorStr();
		if (!empty($_SERVER['REMOTE_ADDR']))			$Result['SourceIP'] = $_SERVER['REMOTE_ADDR'];		// helps with debugging.
		$Result['Error'] = $Result['ErrorNum'] .':'. $Result['ErrorStr'];
		return $Result;
	}


## Subclasses are free to implement/override these methods
## they can also NOT, but set the appropriate static $fxnName var and these functions will call/return the value.
	public function ErrorNum()
	{
		if (is_null($this->cnxnRef))		return 0;
		$fxn = static::$fxnNameErrorNum;
		return $fxn($this->cnxnRef)+0;
	}

	public function ErrorStr()
	{
		if (is_null($this->cnxnRef))		return '';
		$fxn = static::$fxnNameErrorStr;
		return $fxn($this->cnxnRef);
	}

	public function Error()
	{
		$errNum = $this->ErrorNum();
		$errStr = $this->ErrorStr();
		return array($errNum, $errStr);
	}


	public function ErrorMsg($ErrMsg=true)
	{
		if (true === $ErrMsg)				$ErrMsg = $this->Error();
		if (false === $ErrMsg)			return '';
		list($errNum, $errStr) = $ErrMsg;
		return "$errNum:$errStr";
	}


	public function GetQueryError()
	{
		return $this->QErrData;
	}



# A DBError is a failure in a non-query operation, like connect or set database.
	public function GetDBError()
	{
		$ErrInfo = $this->ErrorInfo;
		$ErrInfo['name'] = $this->name;
		$ErrInfo['cnxn_start_ts'] = $this->connStartTime;
		$ErrInfo['num_queries'] = $this->numQueries;
		
		return $ErrInfo;
	}



## -------- Admin Logging --------------

	protected function SendAdminAlert($alertMsg)
	{
		if (array_key_exists('AdminAlerter', $this->Services))
		{
			$AdminAlerter = $this->Services['AdminAlerter'];
			if (is_object($AdminAlerter))						return $AdminAlerter->Alert($alertMsg);
			return $AdminAlerter($alertMsg);
		}
		return false;
	}


	protected function DBUnavailable($alertMsg)
	{
		if (array_key_exists('DBUnavailable', $this->Services))
		{
			$DBUnavailable = $this->Services['DBUnavailable'];
			if (is_object($DBUnavailable))						return $DBUnavailable->Alert($alertMsg);
			return $DBUnavailable($alertMsg);
		}
#		return false;

		error_log("Unable to connect to ". $this::kEngineTypeName ." Server: ". $this->cnxnName .' at host: '. $this->hostID .'. DB:'. $this->defaultDB);
		return false;
	}


}		//	BDB_Base




class BDB_MySQL extends BDB_Base
{
	const kEngineTypeID = 1;
	const kEngineTypeName = 'MySQL';

	protected $host = null;
	protected $port = null;
	protected $socket = null;
	protected $hostID = false;

	protected $username = false;
	protected $password = false;
	protected $defaultDB = false;
	protected $initCmd = false;
	protected $charset = false;
	protected $connectPersistent = false;
	protected $connectCompress = false;
	protected $clientCnxnFlags = false;
	public $threadID = false;

	protected $doAutoReconnect = true;

	protected static $DriverObj = false;
	protected static $reportModeDefault = false;

	protected static $fxnNameDisconnect = 'mysqli_close';
	protected static $fxnNameFreeResult = 'mysqli_free_result';
	protected static $fxnNameFetchAssoc = 'mysqli_fetch_assoc';
	protected static $fxnNameNumRows = 'mysqli_num_rows';
	protected static $fxnNameNumAffectedRows = 'mysqli_affected_rows';
	protected static $fxnNameQuery = 'mysqli_query';
	protected static $fxnNameResultsQuery = 'mysqli_query';

	protected static $sqlBeginTrxn = 'BEGIN WORK';
	protected static $sqlCommitTrxn = 'COMMIT WORK';
	protected static $sqlRollbackTrxn = 'ROLLBACK WORK';



### -------------------------------------------------------------

	public function __construct($name=false, $CnxnParams, $Services=array())
	{
		if (false === self::$DriverObj)
		{
			self::$DriverObj = new mysqli_driver();
			self::$reportModeDefault = self::$DriverObj->report_mode & ~MYSQLI_REPORT_STRICT;
			mysqli_report(self::$reportModeDefault);
		}
		parent::__construct($name, $CnxnParams, $Services);
	}

	public function SetCnxnParams($CnxnParams)
	{
		foreach($CnxnParams as $key => $val)
		{
			switch(strtolower($key))
			{
				case 'host':
				case 'server':
					$this->host = $val;
					break;
				case 'username':
				case 'user':
					$this->username = $val;
					break;
				case 'password':
				case 'pass':
				case 'pwd':
					$this->password = $val;
					break;
				case 'port':
					$this->port = $val+0;
					break;
				case 'socket':
					$this->socket = $val;
					break;
				case 'defaultdb':
				case 'database':
				case 'dbname':
				case 'db':
					$this->defaultDB = trim($val);
					break;
				case 'initcmd':
				case 'init_cmd':
					$this->initCmd = trim($val);
					break;
				case 'charset':
					$this->charset = is_array($val) ? $val : trim($val);
					break;
				case 'persist':
					$this->connectPersistent = (bool)$val;
					break;
				case 'compress':
					$this->connectCompress = (bool)$val;
					break;
				case 'autoreconnect':
					$this->doAutoReconnect = (bool)$val;
					break;
				case 'canfail':
					$this->CnxnOpts['CanFail'] = (bool)$val;
					break;
			}
		}

		$cnxnFlags = 0;
		if ($this->connectCompress)			$cnxnFlags += constant('MYSQLI_CLIENT_COMPRESS');
		if ($cnxnFlags)				$this->clientCnxnFlags = $cnxnFlags;


		if (!empty($this->host))
		{
			$this->hostID = $this->host;
#			if ($this->port)		$this->hostID .= ':'. $this->port
		}
		else
		{
			$this->hostID = empty($this->socket) ? ini_get('mysql.default_socket') : $this->socket;
		}

		return parent::SetCnxnParams($CnxnParams);
	}

/*
TODO - 	'charset_query' => "SET NAMES 'utf8'",
*/

## returns either true or a textual error message
	public function Connect($CnxnOpts=array())
	{
		$this->CnxnOpts = $CnxnOpts;
		$this->SetCnxnParams($CnxnOpts);

		$connectHost = $this->host;
		if ($this->connectPersistent)				$connectHost = 'p:'. $connectHost;
		mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
		$connected = false;
		$CnxnObj = new mysqli();

// the charset parameter can be passed as $charset or array($charset, $collation)
// different methods will require different setup operations.
		$charsetStmt = '';
		if (false !== $this->charset)
		{
			$collation = '';
			if (is_array($this->charset))
			{
				list($charset, $collation) = $this->charset;
			}
			$mysqli->set_charset($charset);
			if (!empty($collation))
			{
				$charsetStmt = "SET NAMES '$charset' COLLATE '$collation'";
			}
		}

		if (false !== $this->initCmd)
		{		// make sure this is a solid cmd prone to never failing.
			$CnxnObj->options(MYSQLI_INIT_COMMAND, $this->initCmd);
		}

		try
		{
			$connected = $CnxnObj->real_connect($connectHost, $this->username, $this->password, null, $this->port, $this->socket, $this->clientCnxnFlags);
		}
		catch (mysqli_sql_exception $e)
		{
			$mysqlErr = $e->getMessage();
			$mysqlErrCode = $e->getCode();
			$alertMsg = 'Failed to connect to MySQL Database on ' . $this->hostID ." - $mysqlErrCode:$mysqlErr\n" . date('Y-m-d h:i:s');
			if (array_key_exists('CanFail', $this->CnxnOpts) && (true == $this->CnxnOpts['CanFail']))				return $this->DBUnavailable($alertMsg);
			$this->SendAdminAlert($alertMsg);
			return $alertMsg;
		}
		mysqli_report(self::$reportModeDefault);
	
		$this->cnxnRef = $CnxnObj;
		$this->connStartTime = microtime(true);
		$this->threadID = $CnxnObj->thread_id;

		if( !empty($this->defaultDB)	&& (false === $CnxnObj->select_db($this->defaultDB) ) )
		{	// Error that we couldn't select the db requested
			return $this->DBError(__function__, $this->name .": Failed to select Database: ". $this->defaultDB);
		}

		if (!empty($charsetStmt))
		{
			
		}

		return true;
	}		//	Connect
	


	public function ServerInfo()
	{
		if (is_null($this->cnxnRef))			return false;
		return $this->cnxnRef->host_info;
	}

	public function Ping($doAutoReconnect=NULL)
	{
		if (is_null($this->cnxnRef))			return false;

		$connected = $this->cnxnRef->ping();
		if ($connected)				return true;

		if (is_null($doAutoReconnect))			$doAutoReconnect = $this->doAutoReconnect;
		
		if (false === $doAutoReconnect)				return false;

		$this->Disconnect();
		$connectResult = $this->Connect($this->CnxnOpts);
		if (false === $connectResult)
		{
			return $this->DBErrorAlert(__function__, "DBPing Reconnect Failed");
		}
		return true;
	}

	public function Epoch()		{	return '1970-01-01 00:00:00'; }


	public function GetInfo($DBInfo=array())
	{
		$DBInfo['thread_id'] = $this->cnxnRef->thread_id;
		return parent::GetInfo($DBInfo);
	}


	public function Host()		{	return $this->host; }

	public function Username()		{	return $this->username; }

	public function QuerySimpleResult($query, $Options=array())
	{
		$this->InitQuery($query, $Options);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');
		if (empty($query))						return $this->HandleQueryParamsError(__function__, 'empty query');

		$this->lastQuery = $query;
		if ($this->logThisQuery)					$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->query($query);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false === $QR)
		{
			return $this->HandleQueryError(__function__);
		}

		if (!$QR->num_rows)
		{
			$this->HandleQueryError(__function__, array('msg'=>'no rows in result set') );
			$data = NULL;		#				error_log("$query\nQueryNumRows:0");
		}
		else
		{
			$data = $QR->fetch_assoc();
			$QR->free();
			$data = current($data);
		}
		return $data;
	}


	public function SelectOne($tableName, $colList, $where='', $orderBy='')
	{
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');
		if (2 == func_num_args())
		{		## ($tableName, $where)
			$where = $colList;
			$colList = '';
		}

		$this->InitQuery($tableName);

		if (empty($colList)) 			$colList = "*";
		
		$this->lastQueryTgtTable = $tableName;
		$this->lastQuery = "SELECT $colList FROM $tableName";
		$this->lastQuery .= empty($where) ? '' : ' WHERE '. $this->BuildWhere($where);
		$this->lastQuery .= empty($orderBy) ? '' : " ORDER BY $orderBy";
		$this->lastQuery .=	' LIMIT 1';

		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false === $QR)
		{
			return $this->HandleQueryError(__function__);
		}

		$RowData = $QR->fetch_assoc();
		$QR->free();
		if (empty($RowData))		$RowData = array();
		return $RowData;
	}		//	SelectOne



## This fxn accepts either 1 or 3 arguments
	public function SelectCell($sqlString)
	{
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');

		$nargs = func_num_args();
		if ( (0 == $nargs) || (2 == $nargs) || ($nargs > 3) )
		{
			return $this->HandleQueryParamsError(__function__, "improper number of arguments: $nargs");
		}
		if (1 == $nargs)			return $this->QuerySimpleResult($sqlString);
		// if there were three or more args, then it's the variant that wants us to construct the sql select

		$table = $sqlString;
		$colName = func_get_arg(1);
		$where = func_get_arg(2);
		$Data = $this->SelectOne($table, $colName, $where);
		if (!empty($Data))		return $Data[$colName];
		return $Data;
	}


	public function SelectList($tableName, $colList, $keyField, $nameField, $where, $orderBy='', $limitInput=0)
	{
		$this->InitQuery($tableName);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');
	
		if (empty($colList)) 	$colList = "*";
		
		$this->lastQueryTgtTable = $tableName;
		$this->lastQuery = "SELECT $colList FROM $tableName";
		$this->lastQuery .= empty($where) ? '' : ' WHERE '. $this->BuildWhere($where);
		$this->lastQuery .= empty($orderBy) ? '' : " ORDER BY $orderBy";
		$this->lastQuery .= $this->MySQL_LimitString($limitInput);

		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false === $QR)
		{
			return $this->HandleQueryError(__function__);
		}

		return $this->FetchResultSetFree($QR, $keyField, $nameField);
	}



	public function QueryOne($query, $Options=array())
	{
		$this->InitQuery($query, $Options);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');

		$this->lastQuery = $query;
		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false === $QR)
		{
			return $this->HandleQueryError(__function__);
		}

		$RowData = $QR->fetch_assoc();
		$QR->free();
		if (empty($RowData))		$RowData = array();
		return $RowData;
	}


	public function QueryList($query, $keyField='', $nameField='')
	{
		$this->InitQuery($query);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');

		$this->lastQuery = $query;
		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false === $QR)
		{
			return $this->HandleQueryError(__function__);
		}

		return $this->FetchResultSetFree($QR, $keyField, $nameField);
	}


## ---------------------------------------------------------
## ---------------------------------------------------------
## Because these functions are not requesting data, they return Result Hashes with varied information.
## -------------------


	public function Insert($tableName, $DataRecord, $Options=array())
	{
		if (!is_array($Options))
		{
			$optVal = $Options;
			$Options = array();
			if (stristr($optVal, 'ignore'))			$Options['IgnoreErrs'] = true;
		}
		$this->InitQuery($tableName, $Options);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');

		$priorityStr = $this->MySQL_PriorityString($Options);

		$this->lastQueryTgtTable = $tableName;
		$this->lastQuery = "INSERT $priorityStr INTO $tableName SET ";
		$this->lastQuery .= $this->PrepareColDataList($DataRecord);
	
		if (array_key_exists('DupKeyUpdate', $Options) )
		{
			$DupKeyData = &$Options['DupKeyUpdate'];
			$this->lastQuery .= ' ON DUPLICATE KEY UPDATE '. $this->PrepareColDataList($DupKeyData);
		}
	
		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->real_query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false === $QR)
		{
			$Result = $this->QErrResultInit(__function__);
			$Result['NumRows'] = 0;
			$this->HandleQueryError(__function__);
		}
		else
		{
			$Result = array('Status' => true, 'Query' => $this->lastQuery, 'QueryTime' => $this->lastQueryTime, );
			$Result['NumRows'] = $numAffRows = $this->cnxnRef->affected_rows;
			if ($numAffRows)			$Result['NewRecID'] = ($this->cnxnRef->insert_id)+0;
		}
		
		return $Result;
	}		//	Insert






	public function Replace($tableName, $DataRecord, $Options=array())
	{
		if (!is_array($Options))		$Options = array($Options => '');
		$this->InitQuery($tableName, $Options);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');

		$this->lastQueryTgtTable = $tableName;
		if (empty($DataRecord))
		{
			return $this->HandleQueryParamsErrorResult(__function__, "Record Data empty for $tableName");
		}

		if (array_key_exists('SafeReplace', $Options))
		{		// asking to do a check first, update if present, insert if not.
			$testKeyField = $Options['SafeReplace'];
			return $this->Upsert($tableName, $DataRecord, $testKeyField, $Options);
		}

		$priorityStr = $this->MySQL_PriorityString($Options);
		$this->lastQuery = "REPLACE $priorityStr INTO $tableName SET ";
		$this->lastQuery .= $this->PrepareColDataList($DataRecord);

		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->real_query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (!$QR )
		{
			$Result = $this->QErrResultInit(__function__);
			$Result['NumRows'] = 0;
			$this->HandleQueryError(__function__);
		}
		else
		{
			$Result = array('Status' => true, 'Query' => $this->lastQuery, 'QueryTime' => $this->lastQueryTime, );
			$Result['NumRows'] = $numAffRows = $this->cnxnRef->affected_rows+0;
			if (1 == $numAffRows)			$Result['NewRecID'] = $this->cnxnRef->insert_id;
		}

		return $Result;
	}		//	Replace




	public function Update($tableName, $DataRecord, $where, $Options=array())
	{
		if (!is_array($Options))
		{		// assume a scalar passed is simply a LIMIT value;
			$Options = array('limit' => ($Options+0));
		}
		$this->InitQuery($tableName, $Options);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');
		if (empty($DataRecord))
		{
			$this->HandleQueryParamsErrorResult(__function__, "Record Data empty for $tableName");
			return array('Status' => false, 'ErrorNum' => -1, 'ErrorStr' => "Record Data empty for $tableName");
		}

		$priorityStr = $this->MySQL_PriorityString($Options);

		$this->lastQueryTgtTable = $tableName;
		$this->lastQuery = "UPDATE $priorityStr $tableName ";
		$this->lastQuery .= " SET ". $this->PrepareColDataList($DataRecord);
		$this->lastQuery .= empty($where) ? '' : ' WHERE '. $this->BuildWhere($where);

		if (array_key_exists('orderby', $Options))
		{
			$this->lastQuery .= " ORDER BY ". $Options['orderby'];
		}
		if ( array_key_exists('limit', $Options) )
		{
			$limitNum = $Options['limit']+0;
			if ($limitNum > 0)			$this->lastQuery .= " LIMIT $limitNum";
		}

		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->real_query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false === $QR)
		{
			$Result = $this->QErrResultInit(__function__);
			$Result['NumRows'] = 0;
			$this->HandleQueryError(__function__);
		}
		else
		{
			$Result = array('Status' => true, 'Query' => $this->lastQuery, 'QueryTime' => $this->lastQueryTime, );
			$Result['NumRows'] = $numAffRows = $this->cnxnRef->affected_rows;
		}
	
		return $Result;
	}		//	Update


	public function DeleteOne($table, $where, $Options=array())
	{
		return $this->DeleteWhere($table, $where, 1, $Options);
	}

	public function DeleteEquals($tableName, $colName, $colValue, $limitNum=0, $Options=array())
	{
		if ('#' == substr($colName, 0, 1))
		{
			$colName = substr($colName, 1);
			$where = $colName .' = '. $colValue;
		}		else 		$where = $colName .' = '. $this->Quote($colValue);
		
		return $this->DeleteWhere($tableName, $where, $limitNum, $Options);
	}

	public function DeleteWhere($tableName, $where, $limitNum=0, $Options=array())
	{
		if (!is_array($Options))	$Options = array();
		$this->InitQuery($tableName, $Options);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');

		$this->lastQueryTgtTable = $tableName;
		$this->lastQuery = "DELETE FROM $tableName";
		$this->lastQuery .= empty($where) ? '' : ' WHERE '. $this->BuildWhere($where);
		$this->lastQuery .= ($limitNum+0) ?  " LIMIT $limitNum" : '';

		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->real_query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (!$QR)
		{
			$Result = $this->QErrResultInit(__function__);
			$Result['NumRows'] = 0;
			$this->HandleQueryError(__function__);
		}
		else
		{
			$Result = array('Status' => true, 'Query' => $this->lastQuery, 'QueryTime' => $this->lastQueryTime, );
			$Result['NumRows'] = $numAffRows = $this->cnxnRef->affected_rows;
		}

		return $Result;
	}		//	DeleteWhere






	public function Upsert($tableName, $DataRecord, $Options=array())
	{
		return false;
		$this->InitQuery($tableName, $Options);

		$testKeyField = $Options['SafeReplace'];
		$testKeyFieldLiteral = '#'.$testKeyField;
		$testKeyFieldActual = $testKeyField;
		if (array_key_exists($testKeyFieldLiteral, $DataRecord))
		{
			$testKeyFieldActual = $testKeyFieldLiteral;
			$testKeyValue = $DataRecord[$testKeyFieldLiteral];
		}
		else
		{
			$testKeyValue = $this->Quote($DataRecord[$testKeyField]);
		}
		$recordSQLWhere = "$testKeyField = $testKeyValue";

		$TestData = $this->SelectOne($tableName, $testKeyField, $recordSQLWhere);
		if (empty($TestData))
		{		// no pre-existing record, do an insert
			$this->lastQuery = "INSERT INTO $tableName SET ";
		}
		else
		{	// pre-existing record, do an update
			unset($DataRecord[$testKeyFieldActual]);
			$this->lastQuery = "UPDATE $tableName SET ";
			$whereClause = " WHERE ($recordSQLWhere) LIMIT 1";
		}


		if (!is_array($Options))			$Options = array();
	
		$priorityStr = $this->MySQL_PriorityString($Options);
	
		$this->lastQuery = "INSERT $priorityStr INTO $tableName SET ";
		$this->lastQuery .= $this->PrepareColDataList($DataRecord);
	
		if (array_key_exists('DupKeyUpdate', $Options) )
		{
			$DupKeyData = &$Options['DupKeyUpdate'];
			$this->lastQuery .= ' ON DUPLICATE KEY UPDATE '. $this->PrepareColDataList($DupKeyData);
		}
	
		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->real_query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (!$QR)
		{
			$Result = $this->QErrResultInit(__function__);
			$Result['NumRows'] = 0;
			$this->HandleQueryError(__function__);
		}
		else
		{
			$Result = array('Status' => true, 'Query' => $this->lastQuery, 'QueryTime' => $this->lastQueryTime, );
			$Result['NumRows'] = $numRows = $this->cnxnRef->affected_rows;
			$Result['NewRecID'] = NULL;
			if ($numRows)			$Result['NewRecID'] = $this->cnxnRef->insert_id;
		}
	
		return $Result;
	}		//	Upsert





### -----------------

	public function Query($query, $Options=array())
	{
		$this->InitQuery($query, $Options);
		if (is_null($this->cnxnRef))				return $this->DBError(__function__, 'No db connection');

		$this->lastQuery = $query;
		if ($this->logThisQuery)				$this->LogQuery(__function__);
		$this->numQueries++;
		$qtime = -microtime(true);
		$QR = $this->cnxnRef->real_query($this->lastQuery);
		$this->lastQueryTime = $qtime + microtime(true);
		if (false === $QR)
		{
			return $this->HandleQueryError(__function__);
		}

		if ($this->cnxnRef->field_count)
		{		// if this query was a select, then return a resut_set object
			return $this->cnxnRef->store_result();
		}
		return $QR;			// otherwise just return true;
	}



	public function LastInsertID($param1=NULL)
	{
		return $this->cnxnRef->insert_id;
	}


	public function EscapeString($data)
	{
		return $this->cnxnRef->real_escape_string($data);
	}

	public function IsConnected()
	{
#		return ( is_object($this->cnxnRef) );
		return ( is_object($this->cnxnRef) && ($this->cnxnRef->thread_id) );
	}


	public function Error()
	{
		return $this->cnxnRef->errno .':'. $this->cnxnRef->error;
	}
	public function ErrorNum()
	{
		return $this->cnxnRef->errno;
	}
	public function ErrorStr()
	{
		return $this->cnxnRef->error;
	}



	public function FetchResultSetFree($QR, $keyField, $nameField)
	{
		$ResultData = $this->FetchResultSet($QR, $keyField, $nameField);
		$QR->free();
		return $ResultData;
	}



	public function NumAffectedRows($QR)
	{
		return $QR->num_rows;
	}



	public function FetchRow($QR)
	{
		if ( !(is_object($QR) || is_resource($QR) ))		return false;
		return $QR->fetch_assoc();
	}
	
	protected static function MySQL_LimitString($limitInput)
	{
		if (empty($limitInput))		return '';
		if (strstr($limitInput, ','))
		{
			list($limitStart, $limitNum) = explode(',', $limitInput);
			$limitStart = intval($limitStart);
			$limitNum = intval($limitNum);
			$limit = "$limitStart,$limitNum";
		}
		else
		{
			$limit = intval($limitInput);
		}

		return " LIMIT $limit";
	}


	protected static function MySQL_PriorityString($Options)
	{
		$priorityStr = '';
		if (array_key_exists('Priority', $Options))
		{
			switch (strtolower($Options['Priority']))
			{
				case 'high': 	return 'HIGH_PRIORITY';
				case 'low': 	return 'LOW_PRIORITY';
#				case 'delayed': 	return 'DELAYED';
			}
		}
		if (array_key_exists('IgnoreErrs', $Options) && (true == $Options['IgnoreErrs']))
		{
			$priorityStr .= ' IGNORE';
		}
		return trim($priorityStr);
	}



## We have some special magic we can perform for a particular MySQL error...
	protected function HandleQueryError($fxn, $AddlData=array())
	{
		$errNum = $this->ErrorNum();
		if (2006 == $errNum)		##	MySQL server has gone away
		{		## this is not a query error.
			return $this->DBError(__function__, 'MySQL server has gone away');
		}

		$this->QErrData['state'] = $this->cnxnRef->sqlstate;

		return parent::HandleQueryError($fxn, $AddlData);
	}		//	MySQL - HandleQueryError
#		if (2006 == $errNum)		##	MySQL server has gone away
#		{
#			@$this->cnxnRef->close();
#			$this->cnxnRef = null;
#			if (!$this->Ping())			return false;
#			
#			$qr = $this->cnxnRef->real_query( (false === $queryString) ? $this->lastQuery : $queryString );
#			if ($qr && $this->cnxnRef->field_count)
#			{		// if this query was a select, then return a resut_set object
#				return $this->cnxnRef->store_result();
#			}
#			return $qr;
#		}



	public function GetDBError()
	{
		$ErrInfo = parent::GetDBError();
		
		$ErrInfo['thread_id'] = $this->threadID;
		$ErrInfo['server'] = $this->ServerInfo();
		
		return $ErrInfo;
	}



}		//	BDB_MySQL
BDB::RegisterEngineType(BDB_MySQL::kEngineTypeID, BDB_MySQL::kEngineTypeName);







### MSSQL
### -----------------------------------------------------------------------------------------------

class BDB_MSSQL extends BDB_Base
{
	const kEngineTypeID = 8;
	const kEngineTypeName = 'MSSQL';

	protected $server = null;
	protected $username = null;
	protected $password = null;
	protected $defaultDB = null;
	protected $connectPersistent = false;

	protected $useAssoc = false;
	
	protected static $fxnNameDisconnect = 'mssql_close';
	protected static $fxnNameFreeResult = 'mssql_free_result';
	protected static $fxnNameNumRows = 'mssql_num_rows';
	protected static $fxnNameQuery = 'mssql_query';


	public function Connect($CnxnOpts=array())
	{
		$this->CnxnOpts = $CnxnOpts;
		if ($this->connectPersistent)
		{
			$connID = mssql_pconnect($this->server, $this->username, $this->password);
		}
		else
		{
			$newLink = false;
			$connID = mssql_connect($this->server, $this->username, $this->password, $newLink);
		}

		if( !$connID )
		{
			$alertMsg = 'Failed to connect to MSSQL Database on ' . $this->hostID ."\n". date('Y-m-d h:i:s');
			if (true == $this->CnxnOpts['CanFail'])				return $this->DBUnavailable($alertMsg);
			$this->SendAdminAlert($alertMsg);
			return $this->DBError(__function__, $alertMsg);
		}

		$this->cnxnRef = $connID;
		$this->connStartTime = microtime(true);
		$this->useAssoc = defined('MSSQL_ASSOC');

		if( !empty($this->defaultDB)	&& (false === mssql_select_db($this->defaultDB, $this->cnxnRef) ) )
		{	// Error that we couldn't select the db requested
			return $this->DBError(__function__, "Failed to select Database: $dbName");
		}
	
		return true;
	}


	public function EscapeString($data)
	{
		return str_replace("'", "''", $data);
	}

	// This function exists because there is no reliable "fetch_assoc" type call for ms_sql type connections
	function CleanMSSQLRow($DataRow)
	{
		if (empty($DataRow))		return array();
		//2 items here:
		//	1. Strip numeric keys
		//	2. do a trim on all remaining columns
		$NewData = array();
		foreach($DataRow as $key => $data)
		{
			if (is_numeric($key))		continue;
			$NewData[$key] = trim($data);
		}
		return $NewData;
	}

	public function Username()		{	return $this->username; }

	public function LastInsertID($table)
	{
		if (empty($table))		return $this->DBErrorAlert(__function__, 'Request on Last Insert ID for blank table name');
		return $this->SelectCell("SELECT ident_current('$table')");
	}

	public function DQuery($query, $Options=array())
	{
		$this->numQueries++;
		return mssql_query($query, $this->cnxnRef);
	}


	public function FetchRow($QR)
	{
		if (!is_resource($QR))		return false;
		return mssql_fetch_array($QR, MSSQL_ASSOC);
	}

	public function NumAffectedRows($QR)
	{
		if (is_null($this->cnxnRef))		return false;
		return $this->SelectCell('select @@ROWCOUNT as RowCount')+0;
	}

	public function Error()
	{
		if (is_null($this->cnxnRef))		return false;
		$errNum = $this->QuerySimpleResult('select @@ERROR as ErrorCode')+0;
		$errStr = 'Error';
		return "$errNum:$errStr";
	}

}		//	BDB_MSSQL
BDB::RegisterEngineType(BDB_MSSQL::kEngineTypeID, BDB_MSSQL::kEngineTypeName);




## ----------------------------------------------------------------------------------
## Query Logging class
##

abstract class BDBLogging
{
	abstract function LogQuery($fxn, $qryText);
}


class BDBLogging_ErrorLog extends BDBLogging
{
	protected $qLogFileTgt = false;

	function LogQuery($fxn, $qryText)
	{
		$log = date('Y-m-d h:i:s') ."\t". $fxn ."\t". $queryString;
		error_log($log);
	}
}


/*
## Change Log




 v1.9.7
 	- the return from HandleQueryParamsError is no longer array-added to the Result hash, but is entirely placed into an err_handled item
 	- BDB::DBList() no longer gathers info for it's result hash, but asks the BDB object to build it. children-objects can interject their own information.
 	- fixed bug in Ping() re: cnxnRef/CnxnObj
 	- connStartTime is now microtime float.

 v1.9.6
 	- MySQL_LimitString now intvals values.

 v1.9.5
 	- Hour component of logged query/error datetime-stamp now uses 24-hour format.
 			Note, the "v" component of the formatting string will only be honored on >= PHP 7.1
 	- mysqli's host_info is a var, not a method.

 v1.9.4
 	- changed some of the logging variables from private to protected to make the logging system work as expected.


 v1.9.3
 	- if attempt to set QueryErrorAlerter from SetCnxnParams fails, emit a generic error_log error.
 	- added shorthand method SelectKeyList


 v1.9.2
 	- added feature to specify query error logging in the CnxnParams passed into Make() via 'ErrorLogging' parameter.
 	- logQueryErrors default value set to true
 	- removed most of the pass by reference for things like escaping values. PHP already does pass by reference/copy on write, so these were unneeded.
 	- improving the handling of QueryError logging to error_log

 v1.9.1
 	- adding query time to QueryLog data block.

 v1.9.0
 	- Completely revamped and made consistent the handling of query error situations.
 			All elements that document the query error are going into a class var: QErrData
 	- lastQueryTgtTable - storing the name of the targeted table name, if it is explicitly specified, in a class variable. any other query fxns will empty that var.
 	- ErrorNum, ErrorStr, ErrorMsg methods
 	- methods that take Query Options now first call InitQuery() which will store those options in a new class variable for other methods to potentially reference.
 			also this method will detect whether an option has been passed to log this particular query (or disable logging)
 	- rolled functionality of CheckForLogQueryDirectives into InitQuery.
 	- Morphed BDB::BDB_BaseQueryLogger to BDB_Base->QueryLogger_ErrorLog
 	- QueryLogger_ErrorLog - made pay attention to and use $qLogFileTgt
 	- added QueryLogger_Memory to accumulate queries
 	- Added GetQueryLog() method to retrieve QueryLogger_Memory generated contents.
 	- Query Result pointer variables are now all consistently named as $QR.
 	- LogDBError changed to DBError
 	- added DBErrorAlert which does the same as DBError but put it into the PHP error log beforehand.
 	- FatalDBError and FatalError changed to DBErrorAlert
 	- standardized lastQueryTime calcs to just before and after call to db driver's query fxn.
 	- HandleQueryParamsError & HandleQueryParamsErrorResult for when our code detects a problem with the parameters passed (empty query/update record)
 	
 v1.8.9
 	- changed name of method CheckForLogQueryCmds to CheckForLogQueryDirectives

 v1.8.8
 	- SelectCell now returns effectively a null value when no record is found from SelectOne query. previously it was returning an empty array.

 v1.8.7
 	- Fixed "Undefined index" bug in SelectCell when SelectOne doesn't locate a record and returns an empty array.

 v1.8.6
 	- Fixed base DeleteOne declaration

 v1.8.5
 	- Added DeleteOne method

 v1.8.4
 	- Fixed some operational code for checking Query Result objects
 	- Made FetchResultSet & FetchResultSetFreepublic
 	- made FetchResultSetFree call Base::FetchResultSet instead of doing work itself
 	- removed errant debug line: if ('Accounts' == $tableName)

 v1.8.3
 	- upon first construction of a MySQL based instance, and copy of a mysqli_driver instance is created and stored in a static member.
 	- upon first construction of a MySQL based instance, the reporting mode of the driver is altered to NOT throw exceptions for errors.
 			We already test for errors on query function returns. Exceptions raised by the driver for SQL errors were being caught by try/catch construct in higher level code.
 			This setting is intended to mask that.
 	- FetchResultSetFree - if * is passed in nameField and keyField is not empty, then the keyField column is retained in the row data AND used as the key for the row in the returned hash.

 v1.8.2
 	- reworked setting QueryLogger to be a bit cleaner; see notes before LogQuery.
 	- BDB_BaseQueryLogger can now target a separate file - call SetQueryLogger with 'error_log_file' and a second param with file path.

 v1.8.1
 	- added Error() as 'Error' to result array.
 	- added LastQuery() accessor
 
 v1.8
 	- modified the MySQL->SetCnxnParams to allow two new config items:
 		- initcmd: this is passed to mysqli->options as MYSQLI_INIT_COMMAND; Make sure it won't fail.
 		- charset: either a single charset value or array(charset, collation)
 	- modified the MySQL->Connect method to create object first before connecting. Sets options like the charset beforehand, then calls real_connect()
 	- MySQL->Connect::real_connect() now includes the clientCnxnFlags which it previously wasn't, now allowing compressed connections.
 	- modified HandleQueryError to create a ErrorData block array containing pertinents for the error at hand. This single item is passed to QueryErrorAlert()
 	- Above ErrorData block also includes mysql->sqlstate.
 	- QueryErrorAlert overrides need to handle this new ErrorData block as their only paramter as the default now does.

 v1.7.5
 	- modified BuildWhere to not prefix WHERE to the returned string and modified code that called that to do that part.
 	- made PrepareColDataOR and PrepareColDataAND public in case someone wanted to use these utility functions.

 v1.7.4
 	- added AsSQLDT($time) method to Base to allow formatting a time according to engine's preferred format
 	- added class constant kSQLDATETIMEFMT to Base which defaults to BDB::kSQLDATETIMEFMT. Used in AsSQLDT() method.

 v1.7.3
 	- SetQueryLogger('error_log') - sets QueryLogger member to true. - signals to QueryLogging code to use PHP error_log()
 	- fixed typo for limit in MySQL_LimitString()

 v1.7.2
 	- Added Username() method and add it to output of BDB::DBList
 
 v1.7.1
 	- Added kSQLDATETIMEFMT constant to BDB
 
 v1.7
 	- Added BDB::DBList() as a form of introspection
 	- added BDB_Base::Host() to retrieve host name/IP
 	- added BDB_Base->numQueries and incrementing code to track number of queries made.
 	- added BDB_Base::Stats() to retrieve statistical values line numQueries.
 	- changed checking for connected status from evaluation if ->cnxnRef is a resource to if the value is not null. in BDB_MySQL, this value is a system/driver generated Object.
 	- tweaked SelectOne to allow only passing two arguments: table & where
 	- moved standalone function BDB_BaseQueryLogger() to be a static method of the control class BDB.
 	- added SetQueryLogger()
 	- added utility function CheckForLogQueryCmds to reduce code clutter.

*/
