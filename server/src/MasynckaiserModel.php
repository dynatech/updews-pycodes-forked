<?php
namespace MyApp;
use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;

class MasynckaiserModel {
    protected $dbconn;

    public function __construct() {
        printf("Initializing %s::%s...\n", __CLASS__, __FUNCTION__);

        //Initialize the database connection
        $this->initDBforMSK();
    }

    public function helloWorld() {
    	echo "ChatMessageModel: Hello World \n\n";
    }

    public function initDBforMSK() {
        echo "Current Working Directory: " . getcwd() . "\n";
        echo "Document Root: " . $_SERVER['DOCUMENT_ROOT'] . "\n";

        // Parse with sections
        // $config_array = parse_ini_file(dirname(__FILE__) . "/../config/config.ini", true);
        $config_array = parse_ini_file(getcwd() . "/config.ini");
        print_r($config_array);

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

        $this->connectMSKDB();
        echo "Switched to schema: senslopedb!\n";

        $this->createMasyncSchemaTargetsTable();
        $this->createMasyncTablePermissionsTable();
    }

    //Connect to senslopedb
    public function connectMSKDB() {
        $success = mysqli_select_db($this->dbconn, "masynckaiser");

        if (!$success) {
            $this->createMSKDB();
        }
    }

    //Create masynckaiser database if it does not exist yet
    public function createMSKDB() {
        $sql = "CREATE DATABASE IF NOT EXISTS masynckaiser";
        if ($this->dbconn->query($sql) === TRUE) {
            echo "Database created successfully\n";
        } else {
            die(__FUNCTION__ . " - Error creating database: " . $this->dbconn->error);
        }
    }

    //Create the masynckaiser_schema_targets table if it does not exist yet
    public function createMasyncSchemaTargetsTable() {
        $sql = "CREATE TABLE IF NOT EXISTS `masynckaiser`.`masynckaiser_schema_targets` (
                  `schema_id` INT NOT NULL AUTO_INCREMENT,
                  `name` VARCHAR(64) NOT NULL,
                  `for_sync` INT NULL DEFAULT 0,
                  PRIMARY KEY (`schema_id`))";

        if ($this->dbconn->query($sql) === TRUE) {
            echo "Table 'masynckaiser_schema_targets' exists!\n";
        } else {
            die(__FUNCTION__ . " - Error creating table: " . $this->dbconn->error);
        }
    }

    //Create the masynckaiser_table_permissions table if it does not exist yet
    public function createMasyncTablePermissionsTable() {
        $sql = "CREATE TABLE IF NOT EXISTS `masynckaiser`.`masynckaiser_table_permissions` (
                  `schema_id` INT NOT NULL AUTO_INCREMENT,
                  `name` VARCHAR(64) NOT NULL,
                  `for_sync` INT NULL DEFAULT 0,
                  PRIMARY KEY (`schema_id`))";

        if ($this->dbconn->query($sql) === TRUE) {
            echo "Table 'masynckaiser_table_permissions' exists!\n";
        } else {
            die(__FUNCTION__ . " - Error creating table: " . $this->dbconn->error);
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
            $this->initDBforMSK();
        }
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

}