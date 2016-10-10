<?php
namespace MyApp;
use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;
use MyApp\MasynckaiserModel;

//Data direction: Server to Client
define("STOC", 0);
//Data direction: Client to Server
define("CTOS", 1);

class Masynckaiser implements MessageComponentInterface {
    protected $clients;

    public function __construct() {
        printf("Initializing %s::%s...\n", __CLASS__, __FUNCTION__);

        //Load the Chat Message Model
        $this->MSKModel = new MasynckaiserModel;
        $this->clients = new \SplObjectStorage;
    }

    public function onOpen(ConnectionInterface $conn) {
        // Store the new connection to send messages to later
        $this->clients->attach($conn);

        echo "New connection! ({$conn->resourceId})\n";
    }

    public function onMessage(ConnectionInterface $from, $msg) {
        $numRecv = count($this->clients) - 1;               
        $decodedText = json_decode($msg);

        if ($decodedText == NULL) {
            echo "Message is not in JSON format ($msg).\n";
            return;
        }
        else {
            echo "Valid data\n";
            echo sprintf('Connection %d sending message "%s" to %d other connection%s' . 
                    "\n", $from->resourceId, $msg, $numRecv, $numRecv == 1 ? '' : 's');

            $action = isset($decodedText->action) ? $decodedText->action : -1;
            $dir = isset($decodedText->dir) ? $decodedText->dir : -1;

            if ($dir == STOC) {
                // This direction will give the server authority to write data on the
                //      client's database. This is the most common feature that will
                //      be utilized for masynckaiser
                echo "Server to Client Data Direction\n";

                // TODO: Very important! Since the direction is from server to client
                //      only... You should reject queries with words like the ff:
                //          - INSERT
                //          - DELETE
                //          - UPDATE
                //          - DROP
                //          - CREATE

                if ($action == "SHOW") {
                    # TODO: Needs schema and table information
                }
                elseif ($action == "SELECT") {
                    $query = isset($decodedText->query) ? $decodedText->query : NULL;

                    if ($query) {
                        // TODO: Check first if the there are restrictions on the schema
                        //      and table that the client wishes to see

                        // TODO: Execute the query request from the client
                        $this->MSKModel->readFromServer($query);
                    }
                }
                elseif ($action == "DESC") {
                    # TODO: Needs schema and table information
                }
                else {
                    echo "Unknown Action\n";
                }

            }
            elseif ($dir == CTOS) {
                // This direction will allow a websocket client to write data on the
                //      websocket server's database. It is critical to screen properly
                //      the name of the client making the request.

                // Note: This functionality won't be available to the quick
                //      prototype version.
                echo "Client to Server Data Direction\n";
            }
            else {
                echo __FUNCTION__ . ": Unknown data direction\n";
            }
        }
    }

    public function onClose(ConnectionInterface $conn) {
        // The connection is losed, remove it, as we can no longer send it messages
        $this->clients->detach($conn);

        echo "Connection {$conn->resourceId} has disconnected\n";
    }

    public function onError(ConnectionInterface $conn, \Exception $e) {
        echo "An error has occurred: {$e->getMessage()}\n";

        $conn->close();
    }
}
