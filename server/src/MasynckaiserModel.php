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
        // $config_array = parse_ini_file(getcwd() . "/config.ini");
        // print_r($config_array);

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

    //Connect to any schema
    public function connectToSchema($schema = NULL) {
        if ($schema) {
            $success = mysqli_select_db($this->dbconn, $schema);

            if (!$success) {
                echo __FUNCTION__ . ": can't connect to " . $schema . "\n";
            }
        } 
        else {
            echo __FUNCTION__ . ": Warning: No schema selected\n";
        }
    }

    //Connect to masynckaiser
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
            echo "Database 'masynckaiser' exists!\n";
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
                  `table_id` INT NOT NULL AUTO_INCREMENT,
                  `schema_id` INT NOT NULL,
                  `name` VARCHAR(45) NOT NULL,
                  `sync_direction` VARCHAR(45) NOT NULL DEFAULT 0,
                  PRIMARY KEY (`table_id`),
                  INDEX `schema_id_idx` (`schema_id` ASC),
                  CONSTRAINT `schema_id`
                    FOREIGN KEY (`schema_id`)
                    REFERENCES `masynckaiser`.`masynckaiser_schema_targets` (`schema_id`)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE)";

        if ($this->dbconn->query($sql) === TRUE) {
            echo "Table 'masynckaiser_table_permissions' exists!\n";
        } else {
            die(__FUNCTION__ . " - Error creating table: " . $this->dbconn->error);
        }
    }

    //Run Any Kind of Query (READ)
    //  Ex: Select, Show, Describe
    //Use this if you are expecting query results
    public function runReadQuery($query) {
        $escQuery = $this->dbconn->escape_string($query);
        echo __FUNCTION__ . " Raw Query: " . $query . "\n";
        echo __FUNCTION__ . " Escaped Query: " . $escQuery . "\n";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($query);

        $result = $this->dbconn->query($query);
        if (!$result) {
            $error_string = __FUNCTION__ . " Error: " . $this->dbconn->error . "\n";
            echo $error_string;
            
            return $error_string;
        } 
        else {
            $results_array = array();
            while ($row = $result->fetch_assoc()) {
                $results_array[] = $row;
            }

            return $results_array;
        }
    }

    //Run Any Kind of SQL Modifying Commands
    //  Ex: Create, Insert, Delete, Update, Alter, Drop
    //Use this if you don't expect query results
    public function runModifierQuery($query) {
        $escQuery = $this->dbconn->escape_string($query);
        echo __FUNCTION__ . " Raw Query: " . $query . "\n";
        echo __FUNCTION__ . " Escaped Query: " . $escQuery . "\n";

        // Make sure the connection is still alive, if not, try to reconnect 
        $this->checkConnectionDB($query);

        $result = $this->dbconn->query($query);
        if (!$result) {
            $error_string = __FUNCTION__ . " Error: " . $this->dbconn->error . "\n";
            echo $error_string;
            
            return $error_string;
        } 
        else {
            return true;
        }
    }

    //Read Queries only
    public function readFromServer($query) {
        // TODO: Very important! Since the direction is from server to client
        //      only... You should reject queries with words like the ff:
        //          - INSERT
        //          - DELETE
        //          - UPDATE
        //          - DROP
        //          - CREATE
        
        //TODO: Apply regex in order to filter out malicious queries that are outside of
        //      the "READ" functionality

        //Modifier commands that aren't allowed in a read only request
        $unallowedCommands = ["CREATE", "INSERT", "DELETE", "UPDATE", "ALTER", "DROP"];
        $trimmedQuery = trim($query);

        foreach ($unallowedCommands as $modifierCmd) {
            $pos = stripos($trimmedQuery, $modifierCmd);
            if ($pos === 0) {
                echo __FUNCTION__ . " Contains unallowed command ($modifierCmd): $query \n";
                return false;
            }
        }

        $array = $this->runReadQuery($query);
        // print_r($array);
        return json_encode($array);
    }

    //Modifier Queries only
    public function writeToServer($query) {
        // TODO: Apply regex in order to filter out unwanted sql commands

        //Modifier commands that aren't allowed in a read only request
        $allowedCommands = ["CREATE", "INSERT", "DELETE", "UPDATE", "ALTER", "DROP"];
        $trimmedQuery = trim($query);

        foreach ($allowedCommands as $modifierCmd) {
            $pos = stripos($trimmedQuery, $modifierCmd);
            if ($pos === 0) {
                $ret = $this->runModifierQuery($query);
                return $ret;
            }
        }

        echo __FUNCTION__ . " Non modifier type query: $query \n";
        return false;
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