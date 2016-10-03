<?php
namespace MyApp;
use Ratchet\MessageComponentInterface;
use Ratchet\ConnectionInterface;
use MyApp\ChatMessageModel;

class ChatterBox implements MessageComponentInterface {
    protected $clients;
    protected $dbconn;
    protected $qiInit;
    protected $chatModel;

    public function __construct() {
        //Load the Chat Message Model
        $this->chatModel = new ChatMessageModel;

        $this->clients = new \SplObjectStorage;
    }

    //TODO: Resilience against Net Connection Loss
    //Create a protocol for checking whether the message was sent to GSM.
    //There should be a function that will attempt to send "PENDING" data
    //  to GSM everytime there is a new connection.

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

            $msgType = $decodedText->type;

            if (($msgType == "smssend") || ($msgType == "smsrcv"))  {
                //save message in DB (maybe create a thread to handle the DB write for the sake of scalability)
                //saving "smssend"
                if ($msgType == "smssend") {
                    echo "Message sent by ChatterBox Users to GSM and the community.\n";

                    //store data in 'smsoutbox' table
                    $recipients = $decodedText->numbers;
                    $sentMsg = $decodedText->msg;
                    $sentTS = $decodedText->timestamp;

                    echo "sentTS = $sentTS \n";

                    $this->chatModel->insertSMSOutboxEntry($recipients, $sentMsg, $sentTS);

                    $displayMsg['type'] = "smssend";
                    $displayMsg['timestamp'] = $sentTS;
                    $displayMsg['user'] = "You";
                    $displayMsg['numbers'] = $recipients;
                    $displayMsg['msg'] = $sentMsg;
                    $displayMsg['gsm_id'] = "UNKNOWN";
                    $displayMsgJSON = json_encode($displayMsg);

                    //broadcast JSON message from GSM to all connected clients
                    foreach ($this->clients as $client) {
                        if ($from !== $client) {
                            // The sender is not the receiver, send to each client connected
                            $client->send($displayMsgJSON);
                        }
                    }                }
                //saving "smsrcv"
                elseif ($msgType == "smsrcv") {
                    echo "Message received from GSM.\n";

                    //store data in 'smsinbox' table
                    $rcvTS = $decodedText->timestamp;
                    $sender = $decodedText->sender;
                    $rcvMsg = $decodedText->msg;

                    if ($this->chatModel->isSenderValid($sender) == false) {
                        echo "Error: sender '$sender' is invalid.\n";
                        return;
                    }

                    $this->chatModel->insertSMSInboxEntry($rcvTS, $sender, $rcvMsg);

                    //Get tags (office, sitename, tags) from number
                    $name = $this->chatModel->getNameFromNumber($sender);

                    $displayMsg['type'] = "smsrcv";
                    $displayMsg['timestamp'] = $rcvTS;
                    $displayMsg['user'] = $sender;
                    $displayMsg['name'] = $name['fullname'];
                    $displayMsg['msg'] = $rcvMsg;
                    $displayMsgJSON = json_encode($displayMsg);

                    //broadcast JSON message from GSM to all connected clients
                    foreach ($this->clients as $client) {
                        if ($from !== $client) {
                            // The sender is not the receiver, send to each client connected
                            $client->send($displayMsgJSON);
                        }
                    }

                    //TODO: Call function to push new incoming message to the 
                    //  quick inbox cache
                    $this->chatModel->addQuickInboxMessageToCache($displayMsg);
                }
            } 
            elseif ($msgType == "smssendgroup") {
                echo "send groups/tag messages...\n";

                //broadcast JSON message from GSM to all connected clients
                foreach ($this->clients as $client) {
                    if ($from !== $client) {
                        // The sender is not the receiver, send to each client connected
                        $client->send($msg);
                    }
                }

                //Get the offices and sitenames info and group message
                $offices = $decodedText->offices;
                $sitenames = $decodedText->sitenames;
                $sentTS = $decodedText->timestamp;
                $sentMsg = $decodedText->msg;

                $displayMsg['type'] = "smssend";
                $displayMsg['timestamp'] = $sentTS;
                $displayMsg['user'] = "You";
                $displayMsg['numbers'] = null;
                $displayMsg['name'] = null;
                $displayMsg['msg'] = $sentMsg;

                //Get contact numbers using group tags
                $contacts = $this->chatModel->getContactNumbersFromGroupTags($offices, $sitenames);

                var_dump($contacts);
                $numContacts = count($contacts['data']);
                $allMsgs = [];

                foreach ($contacts['data'] as $singleContact) {
                    $displayMsg['numbers'] = array($singleContact['number']);
                    $displayMsg['name'] = $singleContact['sitename'] . " " . $singleContact['office'];
                    $displayMsgJSON = json_encode($displayMsg);

                    $this->chatModel->insertSMSOutboxEntry($displayMsg['numbers'], $sentMsg, $sentTS);
                }

                //broadcast JSON message from GSM to all connected clients
                foreach ($this->clients as $client) {
                    if ($from !== $client) {
                        foreach ($contacts['data'] as $singleContact) {
                            $displayMsg['numbers'] = array($singleContact['number']);
                            $displayMsg['name'] = $singleContact['sitename'] . " " . $singleContact['office'];
                            $displayMsg['gsm_id'] = "UNKNOWN";

                            $displayMsgJSON = json_encode($displayMsg);

                            // The sender is not the receiver, send to each client connected
                            $client->send($displayMsgJSON);
                        }
                    }
                }
            }
            elseif ($msgType == "smsloadrequest") {
                echo "Loading messages...";

                //Load the message exchanges between Chatterbox and a number
                $number = $decodedText->number;
                $timestamp = $decodedText->timestamp;

                $exchanges = $this->chatModel->getMessageExchanges($number, $timestamp);
                $from->send(json_encode($exchanges));
            }
            elseif ($msgType == "smsloadrequestgroup") {
                echo "Loading groups/tag messages...";

                //Load the message exchanges between Chatterbox and group selected
                $offices = $decodedText->offices;
                $sitenames = $decodedText->sitenames;

                //Load Message Exchanges using group tags
                $exchanges = $this->chatModel->getMessageExchangesFromGroupTags($offices, $sitenames);

                $from->send(json_encode($exchanges));
            }
            elseif ($msgType == "smsloadquickinboxrequest") {
                //Load latest message from 20 registered numbers
                //Load latest message from 20 unknown numbers
                echo "Loading latest message from 20 registered and unknown numbers for the past 7 days...";

                //Get the quick inbox messages
                //$quickInboxMessages = $this->getQuickInboxMessages();
                $quickInboxMessages = $this->chatModel->getCachedQuickInboxMessages();

                //TODO: Send the quick inbox messages to the 
                $from->send(json_encode($quickInboxMessages));
            }
            elseif ($msgType == "loadofficeandsitesrequest") {
                echo "Loading office and sitename information...";

                //Load the office and sitenames
                $officeAndSites = $this->chatModel->getAllOfficesAndSites();
                $from->send(json_encode($officeAndSites));
            }
            elseif ($msgType == "loadcontactsrequest") {
                echo "Loading contact information...";

                //Load the contacts list
                $contacts = $this->chatModel->getAllContactsList();
                $from->send(json_encode($contacts));
            }
            elseif ($msgType == "loadcommunitycontactrequest") {
                echo "Loading a community contact information...";

                //Load a community contact information
                $sitename = $decodedText->sitename;
                $office = $decodedText->office;

                $commcontact = $this->chatModel->getCommunityContact($sitename, $office);
                $from->send(json_encode($commcontact));
            }
            elseif ($msgType == "loadcontactfromnamerequest") {
                echo "Loading a contact information from name...";

                //Load a community contact information
                $contactname = $decodedText->contactname;

                $contact = $this->chatModel->getContactsFromName($contactname);
                $from->send(json_encode($contact));
            }
            elseif ($msgType == "requestnamesuggestions") {
                echo "Loading name suggestions...";

                //Load a community contact information
                $namequery = $decodedText->namequery;

                //$namesuggestions = $this->getNameSuggestions($namequery);
                $namesuggestions = $this->chatModel->getContactSuggestions($namequery);
                $from->send(json_encode($namesuggestions));
            }
            //Acknowledgement Message from RPi that it has received your message
            elseif ($msgType == "ackrpi") {
                echo "Received Acknowledgment: RPi has received your smsoutbox message...";

                //Acknowledgement Data includes: 
                // timestamp written - to identify what time it was sent by Chatterbox
                // receipient - which number was the information sent to
                $writtenTS = $decodedText->timestamp_written;
                $recipients = $decodedText->recipients;
                $sendStatus = $decodedText->send_status;

                echo "\n\n$writtenTS, $recipients, $sendStatus\n\n";

                //Attempt to Update the smsoutbox entry
                $updateStatus = $this->chatModel->updateSMSOutboxEntry($recipients, 
                                                        $writtenTS, $sendStatus);

                if ($updateStatus >= 0) {
                    //Send the acknowledgment to all connected web socket clients
                    foreach ($this->clients as $client) {
                        if ($from !== $client) {
                            // The sender is not the receiver, send to each client connected
                            $client->send($msg);
                        }
                    }
                }
            }
            //Acknowledgement Message that Chatterbox's outgoing message has been
            //  sent by the GSM already
            elseif ($msgType == "ackgsm") {
                echo "Received Acknowledgment: GSM has sent your smsoutbox message...";

                //Acknowledgement Data includes: 
                // timestamp written - to identify what time it was sent by Chatterbox
                // receipient - which number was the information sent to
                $writtenTS = $decodedText->timestamp_written;
                $recipients = $decodedText->recipients;
                $sendStatus = "SENT";
                $sentTS = $decodedText->timestamp_sent;

                echo "\n\n$writtenTS, $sentTS, $recipients, $sendStatus\n\n";

                //Attempt to Update the smsoutbox entry
                $updateStatus = $this->chatModel->updateSMSOutboxEntry($recipients, 
                                                    $writtenTS, $sendStatus, $sentTS);

                if ($updateStatus >= 0) {
                    //Send the acknowledgment to all connected web socket clients
                    foreach ($this->clients as $client) {
                        if ($from !== $client) {
                            // The sender is not the receiver, send to each client connected
                            $client->send($msg);
                        }
                    }
                }
            }  else if ($msgType == "oldMessage"){
                echo "Loading messages for individual chat";
                $number = $decodedText->number;
                $timestampYou = $decodedText->timestampYou;
                $timestampIndi = $decodedText->timestampIndi;
                $timestamp = $timestampYou.",".$timestampIndi;
                $exchanges = $this->chatModel->getMessageExchanges($number, $timestamp,10);
                $from->send(json_encode($exchanges));
            } else if ($msgType == "oldMessageGroup"){
                echo "Loading messages groups/tag";
                $offices = $decodedText->offices;
                $sitenames = $decodedText->sitenames;
                $yourTimeStamp = $decodedText->lastMessageTimeStampYou;
                $groupTimeStamp = $decodedText->lastMessageTimeStampGroup;
                $lastTimeStamps = $yourTimeStamp.",".$groupTimeStamp;

                $exchanges = $this->chatModel->getMessageExchangesFromGroupTags($offices, $sitenames,$lastTimeStamps,10);
                $from->send(json_encode($exchanges));
            }
            else {
                echo "Message will be ignored\n";
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
