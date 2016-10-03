<?php
namespace MyApp;
use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;

class ChatMessageModel {
    protected $dbconn;

    public function __construct() {
        //Initialize the database connection
        $this->initDBforCB();

        //Cache the initial inbox messages
        $this->qiInit = true;
        $this->getCachedQuickInboxMessages();
    }

    public function helloWorld() {
    	echo "ChatMessageModel: Hello World \n\n";
    }

    public function initDBforCB() {
        //Create a DB Connection
        $host = "localhost";
        $usr = "root";
        $pwd = "senslope";
        $dbname = "senslopedb";

        $this->dbconn = new \mysqli($host, $usr, $pwd);

        if ($this->dbconn->connect_error) {
            die("Connection failed: " . $this->dbconn->connect_error);
        }
        echo "Successfully connected to database!\n";

        $this->connectSenslopeDB();
        echo "Switched to schema: senslopedb!\n";

        $this->createSMSInboxTable();
        $this->createSMSOutboxTable();
    }

    //Connect to senslopedb
    public function connectSenslopeDB() {
        //$success = $this->dbconn->mysqli_select_db("senslopedb");
        $success = mysqli_select_db($this->dbconn, "senslopedb");

        if (!$success) {
            $this->createSenslopeDB();
        }
    }

    //Create database if it does not exist yet
    public function createSenslopeDB() {
        $sql = "CREATE DATABASE senslopedb";
        if ($this->dbconn->query($sql) === TRUE) {
            echo "Database created successfully\n";
        } else {
            die("Error creating database: " . $this->dbconn->error);
        }
    }

    //Create the smsinbox table if it does not exist yet
    public function createSMSInboxTable() {
        $sql = "CREATE TABLE IF NOT EXISTS `senslopedb`.`smsinbox` (
                  `sms_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
                  `timestamp` DATETIME NULL,
                  `sim_num` VARCHAR(20) NULL,
                  `sms_msg` VARCHAR(1023) NULL,
                  `read_status` VARCHAR(20) NULL,
                  `web_flag` VARCHAR(2) NOT NULL DEFAULT 'WU',
                  PRIMARY KEY (`sms_id`))";

        if ($this->dbconn->query($sql) === TRUE) {
            echo "Table 'smsinbox' exists!\n";
        } else {
            die("Error creating table 'smsinbox': " . $this->dbconn->error);
        }
    }

    //Create the smsoutbox table if it does not exist yet
    public function createSMSOutboxTable() {
        $sql = "CREATE TABLE IF NOT EXISTS `senslopedb`.`smsoutbox` (
                  `sms_id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
                  `timestamp_written` DATETIME NULL,
                  `timestamp_sent` DATETIME NULL,
                  `recepients` VARCHAR(1023) NULL,
                  `sms_msg` VARCHAR(1023) NULL,
                  `send_status` VARCHAR(20) NOT NULL DEFAULT 'UNSENT',
                  PRIMARY KEY (`sms_id`))";

        if ($this->dbconn->query($sql) === TRUE) {
            echo "Table 'smsoutbox' exists!\n";
        } else {
            die("Error creating table 'smsoutbox': " . $this->dbconn->error);
        }
    }

    public function filterSpecialCharacters($message) {
        //Filter backslash (\)
        $filteredMsg = str_replace("\\", "\\\\", $message);
        //Filter single quote (')
        $filteredMsg = str_replace("'", "\'", $filteredMsg);

        return $filteredMsg;
    }

    //Check connection and catch SQL that might be clue for MySQL Runaway
    //This is the solution for the "MySQL Runaway Error"
    public function checkConnectionDB($sql = "Nothing") {
        // Make sure the connection is still alive, if not, try to reconnect 
        if (!mysqli_ping($this->dbconn)) {
            echo 'Lost connection, exiting after query #1';

            //Write the ff to the log file
            //  1. Timestamp when the problem occurred
            //  2. The Query to be written
            
            //Append the file
            $logFile = fopen("../logs/mysqlRunAwayLogs.txt", "a+");
            $t = time();
            fwrite($logFile, date("Y-m-d H:i:s") . "\n" . $sql . "\n\n");
            fclose($logFile);

            //Try to reconnect
            $this->initDBforCB();
        }
    }

    //Identify contact number's network
    public function identifyMobileNetwork($contactNumber) {
        try {
            $countNum = strlen($contactNumber);
            //echo "num count = $countNum\n";

            //ex. 09 '16' 8888888
            if ($countNum == 11) {
                $curSimPrefix = substr($contactNumber, 2, 2);
            }
            //ex. 639 '16' 8888888
            elseif ($countNum == 12) {
                $curSimPrefix = substr($contactNumber, 3, 2);
            }

            echo "simprefix: 09$curSimPrefix\n";
            //TODO: compare the prefix to the list of sim prefixes

            //Mix of Smart, Sun, Talk & Text
            $networkSmart = "00,07,08,09,10,11,12,14,18,19,20,21,22,23,24,25,28,29,30,31,
                    32,33,34,38,39,40,42,43,44,46,47,48,49,50,89,98,99";
            //Mix of Globe and TM
            $networkGlobe = "05,06,15,16,17,25,26,27,35,36,37,45,75,77,78,79,94,95,96,97";

            //Globe Number
            if (strpos($networkSmart, $curSimPrefix)) {
                echo "Smart Network!\n";
                return "SMART";
            } 
            elseif (strpos($networkGlobe, $curSimPrefix)) {
                echo "Globe Network!\n";
                return "GLOBE";
            }
            else {
                echo "Unkown Network!\n";
                return "UNKNOWN";
            }
        } catch (Exception $e) {
            echo "identifyMobileNetwork Exception: Unknown Network\n";
            return "UNKNOWN";
        }
    }

    //Insert data for smsinbox table
    public function insertSMSInboxEntry($timestamp, $sender, $message) {
        //filter or check special characters
        $message = $this->filterSpecialCharacters($message);

        $sql = "INSERT INTO smsinbox (timestamp, sim_num, sms_msg, read_status, web_flag)
                VALUES ('$timestamp', '$sender', '$message', 'READ-FAIL', 'WS')";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sql);

        if ($this->dbconn->query($sql) === TRUE) {
            echo "New record created successfully!\n";
        } else {
            echo "Error: " . $sql . "<br>" . $this->dbconn->error;
        }
    }

    //Insert data for smsoutbox table
    public function insertSMSOutboxEntry($recipients, $message, $sentTS = null) {
        //filter or check special characters
        $message = $this->filterSpecialCharacters($message);

        if ($sentTS) {
            $curTime = $sentTS;
        } else {
            $curTime = date("Y-m-d H:i:s", time());
        }

        foreach ($recipients as $recipient) {
            // Identify the mobile network of the current number
            $mobileNetwork = $this->identifyMobileNetwork($recipient);

            echo "$curTime Message recipient: $recipient\n";

            $sql = "INSERT INTO smsoutbox (timestamp_written, recepients, sms_msg, send_status, gsm_id)
                    VALUES ('$curTime', '$recipient', '$message', 'PENDING', '$mobileNetwork')";

            // Make sure the connection is still alive, if not, try to reconnect 
            $this->checkConnectionDB($sql);

            if ($this->dbconn->query($sql) === TRUE) {
                echo "New record created successfully!\n";
            } else {
                echo "Error: " . $sql . "<br>" . $this->dbconn->error;
            }
        }
    }

    //TODO: Update a smsoutbox entry
    public function updateSMSOutboxEntry($recipient=null, $writtenTS=null, $sendStatus=null, $sentTS=null) {
        // validate identifier: recipient
        //Remove non number characters
        $recipient = str_replace("[", "", $recipient);
        $recipient = str_replace("]", "", $recipient);
        $recipient = str_replace("u", "", $recipient);
        $recipient = str_replace("'", "", $recipient);
        $recipient = $this->normalizeContactNumber($recipient);

        if ($this->isSenderValid($recipient) == false) {
            echo "Error: recipient '$recipient' is invalid.\n";
            return -1;
        }

        // TODO: validate identifier: written timestamp
        if ($writtenTS == null) {
            echo "Error: no input for written_timestamp.\n";
            return -1;
        }

        $setCtr = 0;
        $updateQuery = "UPDATE smsoutbox ";
        $whereClause = " WHERE timestamp_written = '$writtenTS' AND recepients like '%$recipient'";

        // validate sendStatus
        if ( ($sendStatus == "PENDING") || ($sendStatus == "SENT-PI") || 
            ($sendStatus == "SENT") || ($sendStatus == "SENT-WSS") ) {
            //compose send status set clause
            $setClause = " SET send_status = '$sendStatus'";
            $setCtr++;
        }
        elseif ($sendStatus == null) {
            // Do nothing
        }
        else {
            echo "Error: invalid send_status.\n";
            return -1;
        }

        // validate sentTS
        if ($sentTS) {
            if ($setCtr > 0) {
                $setClause = $setClause . ", timestamp_sent = '$sentTS'";
            }
            else {
                $setClause = " SET timestamp_sent = '$sentTS'";
            }

            $setCtr++;
        }

        if ($setCtr == 0) {
            echo "Error: No data for updating\n";
            return -1;
        }

        $updateQuery = $updateQuery . $setClause . $whereClause;
        //echo "updateQuery: $updateQuery\n";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($updateQuery);

        if ($this->dbconn->query($updateQuery) === TRUE) {
            echo "record updated successfully!\n";
            return 0;
        } else {
            echo "Error: " . $updateQuery . "<br>" . $this->dbconn->error;
            return -1;
        }
    }

    public function isSenderValid($sender) {
        $patternNoNumbers = '/[^0-9+]/';

        $success = preg_match($patternNoNumbers, $sender, $match);
        if ($success) {
            $patternExceptions='/^LBC|^lbc/';

            if (preg_match($patternExceptions, $sender, $match)) {
                echo "Valid: $sender\n";
                return true;
            }
            else {
                echo "Filter out: $sender\n";
                return false;
            }
        }
        else {
            echo "Valid: All Numbers\n";
            return true;
        }
    }

    public function convertNameToUTF8($name) {
        //Convert the string to utf8 format
        $converted = utf8_decode($name);

        //Replace "?" character with "ñ"
        return str_replace("?", "ñ", $converted);
    }

    public function getCachedQuickInboxMessages($isForceLoad=false) {
        $start = microtime(true);

        $os = PHP_OS;
        $qiResults;

        if (strpos($os,'WIN') !== false) {
            //echo "Running on a windows server. Not using memcached </Br>";
            $qiResults = $this->getQuickInboxMessages();
        }
        elseif ((strpos($os,'Ubuntu') !== false) || (strpos($os,'Linux') !== false)) {
            //echo "Running on a Linux server. Will use memcached </Br>";

            $mem = new \Memcached();
            $mem->addServer("127.0.0.1", 11211);

            //cachedprall - Cached Public Release All
            $qiCached = $mem->get("cachedQI");

            //Load quick inbox results from DB on initialization
            if ( ($this->qiInit == true) || $isForceLoad ) {
                echo "Initialize the Quick Inbox Messages \n";

                $qiResults = $this->getQuickInboxMessages();
                $mem->set("cachedQI", $qiResults) or die("couldn't save quick inbox results");
            } 
            else {
                //Load from cache if no longer from initialization
                $qiResults = $mem->get("cachedQI");
            }
        }
        else {
            //echo "Unknown OS for execution... Script discontinued";
            $qiResults = $this->getQuickInboxMessages();
        }

        //echo json_encode($qiResults) . "\n\n";

        $execution_time = microtime(true) - $start;
        echo "\n\nExecution Time: $execution_time\n\n";

        return $qiResults;
    }

    public function addQuickInboxMessageToCache($receivedMsg) {
        //Get the cached results
        $os = PHP_OS;

        if (strpos($os,'WIN') !== false) {
            //do nothing if on windows
            return;
        }
        elseif ((strpos($os,'Ubuntu') !== false) || (strpos($os,'Linux') !== false)) {
            //echo "Running on a Linux server. Will use memcached </Br>";

            $mem = new \Memcached();
            $mem->addServer("127.0.0.1", 11211);

            //cachedprall - Cached Public Release All
            $qiCached = $mem->get("cachedQI");

            //Load quick inbox results from DB on initialization
            if ($qiCached && ($this->qiInit == true) ) {
                echo "Initialize the Quick Inbox Messages \n";

                $qiResults = $this->getQuickInboxMessages();
                $mem->set("cachedQI", $qiResults) or die("couldn't save quick inbox results");
            } 
            else {
                //delete the oldest message from array
                array_pop($qiCached['data']);  
                //insert latest message to array
                array_unshift($qiCached['data'], $receivedMsg);
                //update the cached quick inbox results
                $mem->set("cachedQI", $qiCached) or die("couldn't save quick inbox results");
            }
        }
        else {
            //do nothing if not on Linux
            return;
        }
    }

    //Return the quick inbox messages needed for the initial display on chatterbox
    public function getQuickInboxMessages() {
        // $start = microtime(true);

        //Get the name of the senders
        $contactsList = $this->getFullnamesAndNumbers();

        $sqlGetLatestMessagePerContact = "
            SELECT timestamp, sim_num as user, sms_msg as msg FROM smsinbox
            WHERE 
            timestamp IN (
                SELECT max(timestamp) FROM smsinbox GROUP BY sim_num
            )
            AND timestamp > (now() - interval 7 day)
            ORDER BY timestamp DESC";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sqlGetLatestMessagePerContact);
        $result = $this->dbconn->query($sqlGetLatestMessagePerContact);

        $ctr = 0;
        $dbreturn = "";
        $fullData['type'] = 'smsloadquickinbox';

        if ($result->num_rows > 0) {
            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $normalizedNum = $this->normalizeContactNumber($row['user']);
                $dbreturn[$ctr]['user'] = $normalizedNum;
                $dbreturn[$ctr]['msg'] = $row['msg'];
                $dbreturn[$ctr]['timestamp'] = $row['timestamp'];
                // $dbreturn[$ctr]['name'] = $this->findFullnameFromNumber($contactsList, $dbreturn[$ctr]['user']);
                $dbreturn[$ctr]['name'] = $this->convertNameToUTF8($this->findFullnameFromNumber($contactsList, $dbreturn[$ctr]['user']));

                //Format for final quick inbox message - user (sim_num), msg, timestamp, name
                echo "fullname: " . $dbreturn[$ctr]['name'] . ", num: " . $dbreturn[$ctr]['user'] . ", ts: " . $dbreturn[$ctr]['timestamp'] . ", msg: " . $dbreturn[$ctr]['msg'] . "\n";

                $ctr = $ctr + 1;
            }

            $fullData['data'] = $dbreturn;
        }
        else {
            echo "0 results\n";
            $fullData['data'] = null;
        }

        //return $fullData;
        echo "JSON DATA: " . json_encode($fullData);
        //echo json_encode($contactsList);

        // $execution_time = microtime(true) - $start;
        // echo "\n\nExecution Time: $execution_time\n\n";

        //Quick Inbox Messages have been reloaded
        $this->qiInit = false;

        return $fullData;
    }

    //Find the Fullname of a contact from a number
    public function findFullnameFromNumber($contactsList, $normalizedNum) {
        // foreach($contactsList as $contact) {
        //     if(strpos($contact['numbers'], $normalizedNum) >= 0) 
        //         return $contact['fullname'];
        // }
        for ($i=0; $i < count($contactsList); $i++) { 
            if (strpos($contactsList[$i]['numbers'], $normalizedNum)) {
                return $contactsList[$i]['fullname'];
            }
        }

        return "unknown";
    }

    //Get Fullnames and numbers in the database
    public function getFullnamesAndNumbers() {
        $sqlGetFullnamesAndNumbers = "
            SELECT
                CONCAT(sitename, ' ', office, ' ', prefix, ' ', firstname, ' ', lastname) as fullname,
                number as numbers
            FROM communitycontacts
            UNION
            SELECT 
                CONCAT(firstname, ' ', lastname) as fullname, 
                numbers
            FROM dewslcontacts
            ORDER BY fullname";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sqlGetFullnamesAndNumbers);
        $result = $this->dbconn->query($sqlGetFullnamesAndNumbers);

        $ctr = 0;
        $dbreturn = "";

        if ($result->num_rows > 0) {
            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $dbreturn[$ctr]['fullname'] = $row['fullname'];
                $dbreturn[$ctr]['numbers'] = $row['numbers'];

                // echo "fullname: ". $row['fullname'] . ", numbers: " . $row['numbers'] . "\n";
                // echo "fullname: ". $dbreturn[$ctr]['fullname'] . ", numbers: " . $dbreturn[$ctr]['numbers'] . "\n";

                $ctr = $ctr + 1;
            }

            // echo json_encode($dbreturn);

            return $dbreturn;
        }
        else {
            echo "0 results\n";
        }
    }

    //Return the message exchanges between Chatterbox and a number
    public function getMessageExchanges($number = null, $timestamp = null, $limit = 20) {
        $ctr = 0;
        if ($number == null) {
            echo "Error: no number selected.";
            return -1;
        } else {
            foreach ($number as $test) {
                //echo "target: $number\n";
                echo "target: $test\n";
                $ctr++;
            }
        }

        $sql = '';
        $sqlTargetNumbers = "";

        //Construct the query for loading messages from multiple numbers
        if ($ctr > 1) {
            for ($i = 0; $i < $ctr; $i++) { 
                $targetNum = $number[$i];

                if ($i == 0) {
                    $sqlTargetNumbersOutbox = "recepients LIKE '%$targetNum' ";
                    $sqlTargetNumbersInbox = "sim_num LIKE '%$targetNum' ";
                } else {
                    $sqlTargetNumbersOutbox = $sqlTargetNumbersOutbox . "OR recepients LIKE '%$targetNum' ";
                    $sqlTargetNumbersInbox = $sqlTargetNumbersInbox . "OR sim_num LIKE '%$targetNum' ";
                }
            }
        } else {
            $targetNum = $number[0];
            $sqlTargetNumbersOutbox = "recepients LIKE '%$targetNum' ";
            $sqlTargetNumbersInbox = "sim_num LIKE '%$targetNum' ";
        }

        //Construct the final query
        if ($limit == 10) {

            $timeStampArray = explode(',', $timestamp);
            $yourLastTimeStamp = $timeStampArray[0];
            $indiLastTimeStamp = $timeStampArray[1];

            if ($yourLastTimeStamp == ""){
                $fetchTimeStamp = "select timestamp_written from smsoutbox where $sqlTargetNumbersOutbox order by timestamp_written desc";
                echo $fetchTimeStamp;
                $this->checkConnectionDB($fetchTimeStamp);
                $result = $this->dbconn->query($fetchTimeStamp);
                $yourLastTimeStamp = $result->fetch_assoc()['timestamp']; 
            }

            if ($indiLastTimeStamp == "") {
                $fetchTimeStamp = "select timestamp from smsinbox where $sqlTargetNumbersInbox order by timestamp desc";
                $this->checkConnectionDB($fetchTimeStamp);
                $result = $this->dbconn->query($fetchTimeStamp);
                $indiLastTimeStamp = $result->fetch_assoc()['timestamp']; 
            }

            $sqlOutbox = "SELECT 'You' as user, sms_msg as msg, 
                            timestamp_written as timestamp FROM smsoutbox timestamp_written inner join (select sms_id from smsoutbox where timestamp_written = '$yourLastTimeStamp' order by sms_id limit 1) x on timestamp_written.sms_id < x.sms_id WHERE $sqlTargetNumbersOutbox";


            $sqlInbox = "SELECT sim_num as user, sms_msg as msg,
                            timestamp as timestamp
                        FROM smsinbox timestamp inner join (select sms_id from smsinbox where timestamp = '$indiLastTimeStamp' order by sms_id limit 1) x on timestamp.sms_id < x.sms_id WHERE $sqlTargetNumbersInbox ";

            $sql = $sqlOutbox."UNION ".$sqlInbox."ORDER BY timestamp desc LIMIT $limit";

        } else {
            if ($timestamp == null) {
                $sqlOutbox = "SELECT 'You' as user, sms_msg as msg, 
                                timestamp_written as timestamp
                            FROM smsoutbox WHERE " . $sqlTargetNumbersOutbox;

                $sqlInbox = "SELECT sim_num as user, sms_msg as msg,
                                timestamp as timestamp
                            FROM smsinbox WHERE " . $sqlTargetNumbersInbox;

                $sql = $sqlOutbox . "UNION " . $sqlInbox . "ORDER BY timestamp desc LIMIT $limit";
            } else {
                $sqlOutbox = "SELECT 'You' as user, sms_msg as msg, 
                                timestamp_written as timestamp
                            FROM smsoutbox WHERE " . $sqlTargetNumbersOutbox . "AND timestamp_written < '$timestamp' ";

                $sqlInbox = "SELECT sim_num as user, sms_msg as msg,
                                timestamp as timestamp
                            FROM smsinbox WHERE " . $sqlTargetNumbersInbox . "AND timestamp < '$timestamp' ";

                $sql = $sqlOutbox . "UNION " . $sqlInbox . "ORDER BY timestamp desc LIMIT $limit";
            }
        }  
        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sql);
        $result = $this->dbconn->query($sql);

        $ctr = 0;
        $dbreturn = "";
        if ($limit == 20){
            $fullData['type'] = 'smsload';
        } else {
            $fullData['type'] = 'oldMessage';
        }
        if ($result->num_rows > 0) {
            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $dbreturn[$ctr]['user'] = $row['user'];
                $dbreturn[$ctr]['msg'] = $row['msg'];
                $dbreturn[$ctr]['timestamp'] = $row['timestamp'];

                $ctr = $ctr + 1;
            }

            $fullData['data'] = $dbreturn;
        }
        else {
            echo "0 results\n";
            $fullData['data'] = null;
        }

        return $fullData;
    }

    //Normalize a contact number
    public function normalizeContactNumber($contactNumber) {
        $countNum = strlen($contactNumber);
        //echo "num count = $countNum\n";

        //ex. 09168888888
        if ($countNum == 11) {
            $contactNumber = substr($contactNumber, 1);
        }
        //ex. 639168888888
        elseif ($countNum == 12) {
            $contactNumber = substr($contactNumber, 2);
        }

        return $contactNumber;
    }

    //Return the contact numbers from the group tags
    public function getContactNumbersFromGroupTags($offices = null, $sitenames = null) {
        $ctr = 0;

        $ctrOffices = 0;
        $subQueryOffices;
        if ($offices == null) {
            echo "Error: no office selected.";
            return -1;
        } else {
            foreach ($offices as $office) {
                if ($ctrOffices == 0) {
                    $subQueryOffices = "('$office'";
                } else {
                    $subQueryOffices = $subQueryOffices . ",'$office'";
                }
                $ctrOffices++;
            }

            $subQueryOffices = $subQueryOffices . ")";
        }
        echo "offices: $subQueryOffices \n";

        $ctrSites = 0;
        $subQuerySitenames;
        if ($sitenames == null) {
            echo "Error: no sitename selected.";
            return -1;
        } 
        else {
            foreach ($sitenames as $site) {
                if ($ctrSites == 0) {
                    $subQuerySitenames = "('$site'";
                } 
                else {
                    $subQuerySitenames = $subQuerySitenames . ",'$site'";
                }
                $ctrSites++;
            }

            $subQuerySitenames = $subQuerySitenames . ")";
        }
        echo "sitenames: $subQuerySitenames \n";

        $sql = '';
        

        //construct query for loading the numbers from the tags selected
        //  by the user
        $sqlTargetNumbers = "SELECT office, sitename, lastname, number 
                            FROM communitycontacts 
                            WHERE office in $subQueryOffices 
                            AND sitename in $subQuerySitenames";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sqlTargetNumbers);
        $result = $this->dbconn->query($sqlTargetNumbers);
        if ($result->num_rows > 0) {
            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $numbers = explode(",", $row['number']);

                foreach ($numbers as $number) {
                    $dbreturn[$ctr]['office'] = $row['office'];
                    $dbreturn[$ctr]['sitename'] = $row['sitename'];
                    $dbreturn[$ctr]['number'] = $number;

                    $ctr = $ctr + 1;
                }
            }

            $contactInfoData['data'] = $dbreturn;
        }
        else {
            echo "0 numbers found\n";
            $contactInfoData['data'] = null;
        }

        return $contactInfoData;
    }

    //Return the message exchanges between Chatterbox and a group
    public function getMessageExchangesFromGroupTags($offices = null, $sitenames = null, $limit = 70) {
        $ctr = 0;

        $tagsCombined = "";

        $ctrOffices = 0;
        $subQueryOffices;
        if ($offices == null) {
            echo "Error: no office selected.";
            return -1;
        } 
        else {
            foreach ($offices as $office) {
                if ($ctrOffices == 0) {
                    $subQueryOffices = "('$office'";
                } else {
                    $subQueryOffices = $subQueryOffices . ",'$office'";
                }

                $tagsCombined = "$tagsCombined $office";
                $ctrOffices++;
            }

            $subQueryOffices = $subQueryOffices . ")";
        }
        echo "offices: $subQueryOffices \n";

        $ctrSites = 0;
        $subQuerySitenames;
        if ($sitenames == null) {
            echo "Error: no sitename selected.";
            return -1;
        } 
        else {
            foreach ($sitenames as $site) {
                if ($ctrSites == 0) {
                    $subQuerySitenames = "('$site'";
                } 
                else {
                    $subQuerySitenames = $subQuerySitenames . ",'$site'";
                }

                $tagsCombined = "$tagsCombined $site";
                $ctrSites++;
            }

            $subQuerySitenames = $subQuerySitenames . ")";
        }
        echo "sitenames: $subQuerySitenames \n";

        $sql = '';
        

        //TODO: construct query for loading the numbers from the tags selected
        //  by the user
        $sqlTargetNumbers = "SELECT office, sitename, lastname, number 
                            FROM communitycontacts 
                            WHERE office in $subQueryOffices 
                            AND sitename in $subQuerySitenames";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sqlTargetNumbers);
        $result = $this->dbconn->query($sqlTargetNumbers);
        if ($result->num_rows > 0) {
            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $numbers = explode(",", $row['number']);

                foreach ($numbers as $number) {
                    $dbreturn[$ctr]['office'] = $row['office'];
                    $dbreturn[$ctr]['sitename'] = $row['sitename'];
                    $dbreturn[$ctr]['number'] = $number;

                    $ctr = $ctr + 1;
                }
            }

            $contactInfoData['data'] = $dbreturn;
        }
        else {
            echo "0 numbers found\n";
            $contactInfoData['data'] = null;
        }

        //echo "JSON output ($ctr): " . json_encode($contactInfoData);

        //Construct the query for loading messages from multiple numbers
        $num_numbers = sizeof($contactInfoData['data']);
        if ($num_numbers >= 1) {
            for ($i = 0; $i < $num_numbers; $i++) { 
                $targetNum = $this->normalizeContactNumber($contactInfoData['data'][$i]['number']);
                $contactInfoData['data'][$i]['number'] = $targetNum;

                if ($i == 0) {
                    $sqlTargetNumbersOutbox = "recepients LIKE '%$targetNum' ";
                    $sqlTargetNumbersInbox = "sim_num LIKE '%$targetNum' ";
                } else {
                    $sqlTargetNumbersOutbox = $sqlTargetNumbersOutbox . "OR recepients LIKE '%$targetNum' ";
                    $sqlTargetNumbersInbox = $sqlTargetNumbersInbox . "OR sim_num LIKE '%$targetNum' ";
                }
            }
        } else {
            $sqlTargetNumbersOutbox = " ";
            $sqlTargetNumbersInbox = " ";
        }

        if ($limit == 10){

            $timeStampArray = explode(',', $lastTimeStamps);
            $yourLastTimeStamp = $timeStampArray[0];
            $groupLastTimeStamp = $timeStampArray[1];

            if ($yourLastTimeStamp == ""){
                $fetchTimeStamp = "select timestamp_written from smsoutbox where $sqlTargetNumbersOutbox order by timestamp_written desc";
                echo $fetchTimeStamp;
                $this->checkConnectionDB($fetchTimeStamp);
                $result = $this->dbconn->query($fetchTimeStamp);
                $yourLastTimeStamp = $result->fetch_assoc()['timestamp']; 
            }

            if ($groupLastTimeStamp == "") {
                $fetchTimeStamp = "select timestamp from smsinbox where $sqlTargetNumbersInbox order by timestamp desc";
                $this->checkConnectionDB($fetchTimeStamp);
                $result = $this->dbconn->query($fetchTimeStamp);
                $groupLastTimeStamp = $result->fetch_assoc()['timestamp']; 
            }

            $sqlOutbox = "SELECT DISTINCT 'You' as user, sms_msg as msg, timestamp_written as timestamp FROM smsoutbox timestamp_written inner join (select sms_id from smsoutbox where timestamp_written = '$yourLastTimeStamp' order by sms_id limit 1) x on timestamp_written.sms_id < x.sms_id WHERE $sqlTargetNumbersOutbox ";


            $sqlInbox = "SELECT DISTINCT sim_num as user, sms_msg as msg,timestamp as timestamp FROM smsinbox timestamp inner join (select sms_id from smsinbox where timestamp = '$groupLastTimeStamp' order by sms_id limit 1) x on timestamp.sms_id < x.sms_id WHERE $sqlTargetNumbersInbox ";

            $sql = $sqlOutbox."UNION ".$sqlInbox."ORDER BY timestamp desc LIMIT $limit";

        } else {
            //Construct the final query
            $sqlOutbox = "SELECT DISTINCT 'You' as user, sms_msg as msg, 
                            timestamp_written as timestamp
                        FROM smsoutbox WHERE $sqlTargetNumbersOutbox AND timestamp_written IS NOT NULL ";

            $sqlInbox = "SELECT DISTINCT sim_num as user, sms_msg as msg,
                            timestamp as timestamp
                        FROM smsinbox WHERE $sqlTargetNumbersInbox AND timestamp IS NOT NULL ";

            $sql = $sqlOutbox . "UNION " . $sqlInbox . "ORDER BY timestamp desc LIMIT $limit";
        }

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sql);
        $result = $this->dbconn->query($sql);

        $ctr = 0;
        $dbreturn = "";
                if ($limit == 10){
            $msgData['type'] = 'oldMessageGroup';
        } else {
            $msgData['type'] = 'smsloadrequestgroup';
        }

        if ($result->num_rows > 0) {
            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $dbreturn[$ctr]['user'] = $row['user'];
                $dbreturn[$ctr]['msg'] = $row['msg'];
                $dbreturn[$ctr]['timestamp'] = $row['timestamp'];

                //Normalize the user's number
                $normalized = $this->normalizeContactNumber($dbreturn[$ctr]['user']);

                //Add "office" and "sitename" data using the "contactInfoData" array
                foreach ($contactInfoData['data'] as $singleContact) {
                    if ($singleContact['number'] == $normalized) {
                        $dbreturn[$ctr]['name'] = $singleContact['sitename'] . " " . $singleContact['office'];
                    }
                }

                if ($dbreturn[$ctr]['user'] == "You") {
                    $dbreturn[$ctr]['name'] = $tagsCombined;
                }

                $ctr = $ctr + 1;
            }

            $msgData['data'] = $dbreturn;
        }
        else {
            echo "0 results\n";
            $msgData['data'] = null;
        }

        //echo json_encode($msgData);

        return $msgData;
    }

    public function getArraySize($arr) {
        $tot = 0;
        foreach($arr as $a) {
            if (is_array($a)) {
                $tot += $this->getArraySize($a);
            }
            if (is_string($a)) {
                $tot += strlen($a);
            }
            if (is_int($a)) {
                $tot += PHP_INT_SIZE;
            }
        }
        return $tot;
    }

    public function getCommunityContact($sitename, $office) {
        if ( ($office == "all") || ($office == null) ) {
            $sql = "SELECT
                        CONCAT(sitename, ' ', office, ' ', prefix, ' ', firstname, ' ', lastname) as fullname,
                        number as numbers
                    FROM communitycontacts
                    WHERE sitename like '%$sitename%'
                    ORDER BY fullname";
        } else {
            $sql = "SELECT
                        CONCAT(sitename, ' ', office, ' ', prefix, ' ', firstname, ' ', lastname) as fullname,
                        number as numbers
                    FROM communitycontacts
                    WHERE sitename like '%$sitename%' AND office = '$office'
                    ORDER BY fullname";
        }

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sql);
        $result = $this->dbconn->query($sql);

        $ctr = 0;
        $dbreturn = "";
        $fullData['type'] = 'loadcommunitycontact';

        if ($result->num_rows > 0) {
            $fullData['total'] = $result->num_rows;
            echo $result->num_rows . " results\n";

            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $dbreturn[$ctr]['fullname'] = $this->convertNameToUTF8($row['fullname']);
                $dbreturn[$ctr]['numbers'] = $row['numbers'];

                $ctr = $ctr + 1;
            }

            $fullData['data'] = $dbreturn;
            echo "data size: " . $this->getArraySize($dbreturn);
        }
        else {
            echo "0 results\n";
            $fullData['data'] = null;
        }

        //echo json_encode($fullData);
        return $fullData;
    }

    public function getContactsFromName($queryName) {
        $sql = "SELECT * FROM
                    (SELECT
                        CONCAT(sitename, ' ', office, ' ', prefix, ' ', firstname, ' ', lastname) as fullname,
                        number as numbers
                    FROM communitycontacts
                    UNION
                    SELECT 
                        CONCAT(firstname, ' ', lastname) as fullname, 
                        numbers
                    FROM dewslcontacts
                    ORDER BY fullname) as fullcontacts
                WHERE
                    fullname LIKE '%$queryName%'";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sql);
        $result = $this->dbconn->query($sql);

        $ctr = 0;
        $dbreturn = "";
        $fullData['type'] = 'loadcommunitycontact';

        if ($result->num_rows > 0) {
            $fullData['total'] = $result->num_rows;
            echo $result->num_rows . " results\n";

            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $dbreturn[$ctr]['fullname'] = $this->convertNameToUTF8($row['fullname']);
                $dbreturn[$ctr]['numbers'] = $row['numbers'];

                $ctr = $ctr + 1;
            }

            $fullData['data'] = $dbreturn;
            echo "data size: " . $this->getArraySize($dbreturn);
        }
        else {
            echo "0 results\n";
            $fullData['data'] = null;
        }

        //echo json_encode($fullData);
        return $fullData;
    }

    //Get contact name from number
    public function getNameFromNumber($contactNumber) {
        //normalize the number
        $normalized = $this->normalizeContactNumber($contactNumber);

        //TODO: create query to get name from the contact number if it exists
        $sql = "SELECT * FROM 
                    (SELECT
                        CONCAT(sitename, ' ', office, ' ', prefix, ' ', firstname, ' ', lastname) as fullname,
                        number as numbers
                    FROM communitycontacts
                    UNION
                    SELECT
                        CONCAT(firstname, ' ', lastname) as fullname, 
                        numbers
                    FROM dewslcontacts) as contactNames
                WHERE
                    numbers LIKE '%$normalized%'";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sql);
        $result = $this->dbconn->query($sql);

        $dbreturn = [];
        if ($result->num_rows > 0) {
            $ctr = 0;
            // output data of each row
            while ($row = $result->fetch_assoc()) {
                if ($ctr == 0) {
                    $dbreturn['fullname'] = $row['fullname'];
                }
                else {
                    $dbreturn['fullname'] = $dbreturn['fullname'] . ', ' . $row['fullname'];
                }

                $ctr++;
            }

            echo "data size: " . $this->getArraySize($dbreturn);
        }
        else {
            echo "0 results\n";
            $dbreturn['fullname'] = "unknown";
        }

        echo json_encode($dbreturn);
        return $dbreturn;
    }

    public function getContactSuggestions($queryName) {
        $sql = "SELECT * FROM
                    (SELECT
                        CONCAT(sitename, ' ', office, ' ', prefix, ' ', firstname, ' ', lastname) as fullname,
                        number as numbers
                    FROM communitycontacts
                    UNION
                    SELECT 
                        CONCAT(firstname, ' ', lastname) as fullname, 
                        numbers
                    FROM dewslcontacts
                    ORDER BY fullname) as fullcontacts
                WHERE
                    fullname LIKE '%$queryName%'
                    OR numbers LIKE '%$queryName%'";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sql);
        $result = $this->dbconn->query($sql);

        $ctr = 0;
        $dbreturn = "";
        $fullData['type'] = 'loadnamesuggestions';

        if ($result->num_rows > 0) {
            $fullData['total'] = $result->num_rows;
            echo $result->num_rows . " results\n";

            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $dbreturn[$ctr]['fullname'] = $this->convertNameToUTF8($row['fullname']);
                $dbreturn[$ctr]['numbers'] = $row['numbers'];

                $ctr = $ctr + 1;
            }

            $fullData['data'] = $dbreturn;
            echo "data size: " . $this->getArraySize($dbreturn);
        }
        else {
            echo "0 results\n";
            $fullData['data'] = null;
        }

        //echo json_encode($fullData);
        return $fullData;
    }

    public function getNameSuggestions($queryName) {
        $sql = "SELECT * FROM
                    (SELECT
                        CONCAT(sitename, ' ', office, ' ', prefix, ' ', firstname, ' ', lastname) as fullname
                    FROM communitycontacts
                    UNION
                    SELECT 
                        CONCAT(firstname, ' ', lastname) as fullname
                    FROM dewslcontacts
                    ORDER BY fullname) as fullcontacts
                WHERE
                    fullname LIKE '%$queryName%'
                    OR numbers LIKE '%$queryName%'";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sql);
        $result = $this->dbconn->query($sql);

        $ctr = 0;
        $dbreturn = "";
        $fullData['type'] = 'loadnamesuggestions';

        if ($result->num_rows > 0) {
            $fullData['total'] = $result->num_rows;
            echo $result->num_rows . " results\n";

            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $dbreturn[$ctr] = $this->convertNameToUTF8($row['fullname']);

                $ctr = $ctr + 1;
            }

            $fullData['data'] = $dbreturn;
            echo "data size: " . $this->getArraySize($dbreturn);
        }
        else {
            echo "0 results\n";
            $fullData['data'] = null;
        }

        //echo json_encode($fullData);
        return $fullData;
    }

    //Return the normalized contact list for both DEWSL and Community
    //currently exceeds the web socket bandwidth (9115 bytes)
    public function getAllContactsList() {
        $sql = "SELECT
                    CONCAT(sitename, ' ', office, ' ', prefix, ' ', firstname, ' ', lastname) as fullname,
                    number as numbers
                FROM communitycontacts
                UNION
                SELECT 
                    CONCAT(firstname, ' ', lastname) as fullname, 
                    numbers
                FROM dewslcontacts
                ORDER BY fullname";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sql);
        $result = $this->dbconn->query($sql);

        $ctr = 0;
        $dbreturn = "";
        $fullData['type'] = 'loadcontacts';

        if ($result->num_rows > 0) {
            $fullData['total'] = $result->num_rows;
            echo $result->num_rows . " results\n";

            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $dbreturn[$ctr]['fullname'] = $row['fullname'];
                $dbreturn[$ctr]['numbers'] = $row['numbers'];

                $ctr = $ctr + 1;
            }

            $fullData['data'] = $dbreturn;
            echo "data size: " . $this->getArraySize($dbreturn);
        }
        else {
            echo "0 results\n";
            $fullData['data'] = null;
        }

        //echo json_encode($fullData);
        return $fullData;
    }

    //This will only be called only one time and only if the user clicks
    //  on the "advanced search option"
    public function getAllOfficesAndSites() {
        $fullData['type'] = 'loadofficeandsites';

        //Get the list of offices from the community contacts list
        $sqlOffices = "SELECT DISTINCT office FROM communitycontacts";
        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sqlOffices);
        $result = $this->dbconn->query($sqlOffices);

        $ctr = 0;
        $returnOffices = "";

        if ($result->num_rows > 0) {
            $fullData['total_offices'] = $result->num_rows;
            echo $result->num_rows . " results\n";

            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $returnOffices[$ctr] = $row['office'];

                $ctr = $ctr + 1;
            }

            $fullData['offices'] = $returnOffices;
            echo "Offices data size: " . $this->getArraySize($returnOffices) . "\n";
        }
        else {
            echo "0 results for offices\n";
            $fullData['offices'] = null;
        }

        //Get the list of sitenames from the community contacts list
        $sqlSitenames = "SELECT DISTINCT sitename FROM communitycontacts";
        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($sqlSitenames);
        $result = $this->dbconn->query($sqlSitenames);

        $ctr = 0;
        $returnSitenames = "";

        if ($result->num_rows > 0) {
            $fullData['total_sites'] = $result->num_rows;
            echo $result->num_rows . " results\n";

            // output data of each row
            while ($row = $result->fetch_assoc()) {
                $returnSitenames[$ctr] = $row['sitename'];

                $ctr = $ctr + 1;
            }

            $fullData['sitenames'] = $returnSitenames;
            echo "Sitenames data size: " . $this->getArraySize($returnSitenames) . "\n";
        }
        else {
            echo "0 results for sitenames\n";
            $fullData['sitenames'] = null;
        }

        // echo json_encode($fullData);
        return $fullData;
    }
}