/**
 * @file can_local_due.ino
 * @author K Razon, J Serrato, R Canseco
 * @date 4 Dec 2015
 * @brief Controller Area Network (CAN) related code of Due based datalogger for the SENSLOPE PROJECT.
 *
 * This file requires due_can.h for it to run. This file contains CanInitialize to initialize the CAN.
 * A messaging function Broadcast_CMD is used to gather data from the nodes. A CAN message is sent to
 * all the nodes connected, and a reply is expected. Also contains the functions to process and arrange
 * the aggregated data from CAN Frames to a string placed in a buffer. 
 *
 * @bug Only compiles in the Arduino IDE used by the original author ( Canseco ).
 * 
 */

/*!
\file  can_local_due.ino
 
\author Raphael Victor L. Canseco
\umangkin Kennex H. Razon
\date July 2015
\version 2.0 
 
\brief contains the helper functions for CAN communication for arduino DUE
  
 Board: Arduino DUE
 This file requires due_can.h for it to run. This file contains CanInitialize to initialize the CAN. In this version only
 CAN1 of the DUE is used. Two messaging functions are also contained in this file. MasterBroadcast and MasterNodeSpecific
 Both functions sends a CAN message then expects a CAN message reply. The messages sent and received are in extended
 identifier mode and would not allow standard identifier. 
 
 @warning	requires due_can.h
 
 version 1.2 \n
		adds MasterBroadcastV2 that does a retry just in case some nodes were slow \n
		adds CanGetRcvArraySize that gets the size of the output array by using the end node with id = 0
		adds printCanData that prints the data from the array
		
 version 1.1 \n
	 adds the overloaded functions for MasterBroadcast and MasternodeSpecific for separating the getting the data and the \n
         sending of the command. \n
	added the ArrangeCanMsgOrder that arranges the rcvd data using selection sort from smallest id to largest id \n
	added the printCanData that prints data that are in the array \n 
 
 
 */

#ifndef due_can_h
#define due_can_h
#include <due_can.h>
#endif

#include <SD.h>
#include <spi.h>

#include <string.h>
#define MASTERMSGID						1   //!< The message id used by the master in all CAN communications
#define MASTERBROADCASTIDLIMIT			100 //!< lower than this would be treated as broadcast, higher than this would be treated as node specific
#define CANTIMEOUT						65000 //!< The amount of time in miliseconds before the communication would time out
//unsigned int canRcvArrySize;
//RX_CAN_FRAME incoming;						//!< global temporary holder for messages	

#define CHECKAVAILABLECMD		99
#define CANARRAYMAXSIZE        100
RX_CAN_FRAME can_default_buffer[CANARRAYMAXSIZE];

unsigned long GIDTable2[CANARRAYMAXSIZE][2];
unsigned long SOMSNodeSpecificTable[CANARRAYMAXSIZE][2];

extern unsigned int samplingMaxNumOfRetry;


/*end */


 /**

@brief       starts a broadcast to all nodes and waits for the reply from the nodes
@version 1.1  appends a dummy data \n

	This function assumes a master and nodes connection where the first databyte is used for the identifier
of the message. The nodes would look into the first databyte to check what command is being sent to them
The message id used of the master is 1 and the nodes are configured to accept a CAN message with messageid
of 1. The reply of the nodes would be dependent on the node but it is prescribed that the node would
echo the byte0 sent by the master.This function also assumes an extended identifier communication.
This function employs a timeout so that it exits if not all nodes responded within a determined time. The
time is specified in timeout parameter

@note
For reference comms format																				\n
source\	destination\	message id\	byte 0\	byte1\	byte 2\	byte 3\	byte 4\	byte 5\	byte 6\	byte 7\		\n
node specific																							\n
master\	nodes\	1\	msg identifier\	unique id\	unique id\	data0\	data1\	data2\	data3\	data4\		\n
broadcast																								\n
master\	node\	1\	msg identifier\	X\	data0\	data1\	data2\	data3\	data4\	data5\					\n
@endnote 

@param [in]		toSend					pointer to message identifier/command identifier uses toSend->data2 upto toSend->data8 for data 
@param [out]	canRcvDataArray                                 array of datatype RX_CAN_FRAME where all the data would be placed
@param [in]		numOfnodes				number of expected nodes also to prevent buffer overflow of array
@param [in]		timeout					number of miliseconds before timeout would occur

@retval 2	 broadcast timed out before all nodes responded
@retval 1    msgId not within the range of broadcast
@retval  0	success

@code
unsigned int command;
unsigned int numberofnodes;

RX_CAN_FRAME can_rcv_data_array[numberofnodes];              //declare receive array
TX_CAN_FRAME can_snd_data;
TX_CAN_FRAME *ptr;
ptr = can_snd_data;								 
id = 10;

ptr->data1 = id;
ptr->data2 = 1;
ptr->data3 = 4;
ptr->data4 = 5;

command = 1;
numberofnodes = 40;
timeout = 200;
MasterBroadcast(ptr,can_rcv_data_array,numberofnodes,timeout);

@endcode
 
*/

int tid = 0;
int t0 = 0;
int t1 = 0;
int retry_find_cnt = 0;
int prev_cmd = 0; /*!< The last can command sent. Initially set to 0. Will be used for comparison. */
int store_frame = 0;  // flag to call store_can_frame function
RX_CAN_FRAME* t_dataptr = temp_can_rcv_data_array;  // data frame temp storage // set as global para accessible sa lahat
int unique_id_cnt =0;

/*! \brief GET_DATA function to aggregate data from column to a buffer.
 *         Brief description continued.
 *
 * 	The GET_DATA function aggregates data gathered from the sensor column and places them in a buffer.
 *It also manages errors encountered in data gathering. 
 *
 *@param [in]		*columnPointer					pointer of buffer to be used as temporary storage of aggregated data
 *@param [in]		CMD								Command to be issued to the column. Determines how the data is taken from the nodes ( "polling" or broadcast )
 */

void GET_DATA(char *columnPointer, int CMD){
//                Serial1.print("ARQWAIT");
//                Serial.println("ARQWAIT -- GET_DATA 1st ");
		int cmd_error =0;
		int retry_no_data_from_column_cnt =0;
		int retry_because_repeating_cnt =0;
		can_snd_data_array->data[0] = CMD; //command with long
//                Serial1.print("ARQWAIT");
//                Serial.println("ARQWAIT -- GET_DATA 2nd ");
		if (CMD > 100){
			if (PRINT_MODE == 1) {Serial.println("  -- function called: GET_DATA( polling )");}
			CanInitialize(40000,temp_can_rcv_data_array,numberofnodes);
			CanInitialize(40000,can_rcv_data_array,numberofnodes);
			Broadcast_CMD(can_snd_data_array, can_rcv_data_array , numOfNodes , TIMEOUT,t_unique_ids);

		} else {
			if (PRINT_MODE == 1) {Serial.println("  -- function called: GET_DATA( broadcast )");}
			cmd_error = 3; // force while condition
			while( cmd_error >= 3 ) {
				CanInitialize(40000,can_rcv_data_array,numberofnodes);
				cmd_error = Broadcast_CMD(can_snd_data_array, can_rcv_data_array , numOfNodes , TIMEOUT);
				if (cmd_error == 3 ){
					Serial.println("  -- retried: daming repeating");
					retry_because_repeating_cnt++;
				} else if (cmd_error ==  4 ){
					Serial.println("  -- retried: kulang data");
					retry_no_data_from_column_cnt++; 
				} else {
					break; // for redundancy purposes 
				}
				if (retry_because_repeating_cnt > REPEATING_FRAMES_RETRY_LIMIT ){
					Serial.print(" -- Retry limit reached. Madaming repeating "); Serial.println(retry_because_repeating_cnt);
					break;
				} else if (retry_no_data_from_column_cnt > NO_COLUMN_LIMIT ){
					Serial.print(" -- Retry limit reached. Kulang yung frames. "); Serial.println(retry_no_data_from_column_cnt);
					// dapat galing sa sd yung ids
					// EDIT NA LANG NUNG COMMAND TAPOS OKAY NA. :)
					//Broadcast_CMD(can_snd_data_array, can_rcv_data_array , numOfNodes,TIMEOUT,t_unique_ids); // dapat GIDTable[x][1]
					break;
		
		               }
			} 
		}
		Serial.println("  Final temp_can_rcv_data_array");
		printRX_Frame(temp_can_rcv_data_array,CanGetRcvArraySize(temp_can_rcv_data_array),1);
		columnPointer[0] = 0;
		sizeofarray = CanGetRcvArraySize(temp_can_rcv_data_array);
		append_array_to_message(temp_can_rcv_data_array,columnPointer,sizeofarray);
		ArrangeCanMsgOrder(can_default_buffer, 0);
		for(int x=0;x<sizeofarray;x++){
			append_to_end_of_can_array(can_default_buffer,&temp_can_rcv_data_array[x]);	
		}
}

unsigned int Broadcast_CMD( TX_CAN_FRAME* toSend, RX_CAN_FRAME* canRcvDataArray ,unsigned int numOfnodes, unsigned long timeout, int id_array[]) {        
	RX_CAN_FRAME *dataptr,*c_dataptr;
	TX_CAN_FRAME *txdataptr;
	int ctrid, k=0,x;
	unsigned int msgId;
	int error;
	unsigned int indexCnt,rx_error_cnt,tx_error_cnt ;
	unsigned long msgHolder[8];
	char *columnPointer = columnData;
	bool ctimeout;  
	int first_frame = 1;
	int repeating = 0;  
	int illegal_limit = 0;
	int uid = 0;
	int retry = 0;
        int wrongID = 0;
	if (PRINT_MODE == 1) {Serial.println("function: Broadcast_CMD() -- called"); }	
	CAN.enable();
	Turn_on_column();  
	error = 0;
	if(msgId > MASTERBROADCASTIDLIMIT){										//means outside broadcast range
			error = 1;
			CAN.disable();
			return error;
	}
	dataptr = canRcvDataArray;
	for (int ctrid = 0; ctrid < numOfNodes; ctrid++) {
			uid = id_array[ctrid];
			if (PRINT_MODE == 1){Serial.print("-- -- -- id_array[ "); Serial.print(ctrid); Serial.print(" ] = "); Serial.println(uid);}
			txdataptr = toSend;
			msgId = txdataptr->data[0];											//extract message ID
			msgHolder[0] = msgId;	msgHolder[1] = uid >> 8;	
			msgHolder[2] = uid & 0xFF;	msgHolder[3] = txdataptr->data[3];	
			msgHolder[4] = txdataptr->data[4];	msgHolder[5] = txdataptr->data[5];
			msgHolder[6] = txdataptr->data[6];	msgHolder[7] = txdataptr->data[7];

			CAN.mailbox_set_datah(1,  msgHolder[4] + (msgHolder[5] << 8) + (msgHolder[6] << 16) + (msgHolder[7]<<24) );
			CAN.mailbox_set_datal(1, msgHolder[0] + (msgHolder[1] << 8) + (msgHolder[2] << 16) + (msgHolder[3]<<24) );
			CAN.mailbox_set_datalen(1,8);
			CAN.enable_interrupt(CAN_IER_MB1);
			CAN.enable_interrupt(CAN_IER_MB0); 
			CAN.global_send_transfer_cmd(CAN_TCR_MB1); // sends command

			if (CanCheckTimeout(timeout) == true){                                // If no data, print timeout for node id
				if (PRINT_MODE == 1){
					Serial.print("-- -- -- Timed Out Polling node  ");
					Serial.println(uid);
				}
				if ( retry <= 1){
                                        //Serial1.print("ARQWAIT");
                                        ctrid--;
					Serial.print("  Resend cmd for  "); Serial.println(uid);
                                        retry++;
					continue;
					
				} else {
                                        retry = 0;
                                        Serial1.print("ARQWAIT");
				        Serial.println("ARQWAIT ...");
                                        continue;
                                }
			}
			CAN.get_rx_buff(&incoming);
			if (incoming.data[0] != msgId){
					Serial.println("  !@#$%^ illegal msgId FOUND!!");
					ctrid--;
					illegal_limit++; 
					if ( illegal_limit >= 15 ){ 				// consider this as repeating frame and resend cmd.
							Turn_off_column();
							CAN.disable();				        // reset can controller	
							dataptr->id = 0;					// para di maisama sa ipaparse na data
							dataptr->data[0] = 0;	dataptr->data[1] = 0;
							dataptr->data[2] = 0;	dataptr->data[3] = 0;
							dataptr->data[4] = 0;	dataptr->data[5] = 0;
							dataptr->data[6] = 0;	dataptr->data[7] = 0;
							error = 3;
							return error;
					}
					continue;
			}
			if (incoming.id != uid){
					Serial.println("  !@#$^& Wrong ID found.");
                                        wrongID++;
                                        Serial1.print("ARQWAIT");
                                        if ( wrongID <= 100 ){
					    ctrid--;
                                        }
					continue;
			}
                        retry = 0; //reset retry value for the next nodes.
			store_can_frame(incoming.id,incoming.data[0],incoming.data[1],incoming.data[2],incoming.data[3],incoming.data[4],incoming.data[5],incoming.data[6],incoming.data[7]);
			dataptr->id = incoming.id;
			dataptr->dlc = incoming.dlc;
			dataptr->data[0]=incoming.data[0];	dataptr->data[1]=incoming.data[1];
			dataptr->data[2]=incoming.data[2];	dataptr->data[3]=incoming.data[3];
			dataptr->data[4]=incoming.data[4];	dataptr->data[5]=incoming.data[5];
			dataptr->data[6]=incoming.data[6];	dataptr->data[7]=incoming.data[7];
			dataptr++;
			//Serial.print( "CanGetRcvArraySize(canRcvDataArray) ="); Serial.println(CanGetRcvArraySize(canRcvDataArray));
			//Serial.print(" numOfNodes = "); Serial.println(numOfNodes); 

	}	
	dataptr->id = 0;                                 //terminating char?    
	dataptr->data[0] = 0;	dataptr->data[1] = 0;
	dataptr->data[2] = 0;	dataptr->data[3] = 0;
	dataptr->data[4] = 0;	dataptr->data[5] = 0;
	dataptr->data[6] = 0;	dataptr->data[7] = 0;
	CAN.disable();
	error = 0;
	return error;
}

int Broadcast_CMD( TX_CAN_FRAME* toSend,RX_CAN_FRAME* canRcvDataArray,unsigned int numOfnodes, unsigned long timeout) {    
	if (PRINT_MODE == 1) {Serial.println("  -- function: Broadcast_CMD() -- called"); } 
        //Serial1.print("ARQWAIT");
        //Serial.println("ARQWAIT -- Broadcast_CMD");
	RX_CAN_FRAME *dataptr,*c_dataptr;
	TX_CAN_FRAME *txdataptr;
	int ctrid, k=0,x;
	unsigned int msgId;
	int error;
	unsigned int rx_error_cnt,tx_error_cnt ;
	unsigned long msgHolder[8];
	char *columnPointer = columnData;
	bool ctimeout;
	int r_f_count = 0; // repeating frame count
	int first_frame = 1;
	int repeating = 0;    
	int illegal_limit = 0;
	//char temp[10];
	
	CAN.enable();
	Turn_on_column();
	error = 0;
	if(msgId > MASTERBROADCASTIDLIMIT){										//means outside broadcast range
		error = 1;
		CAN.disable();
		return error;
	}
	txdataptr = toSend;
	msgId = txdataptr->data[0];											//extract message ID
	msgHolder[0] = msgId;
	msgHolder[1] = txdataptr->data[1];
	msgHolder[2] = txdataptr->data[2];
	msgHolder[3] = txdataptr->data[3];
	msgHolder[4] = txdataptr->data[4];
	msgHolder[5] = txdataptr->data[5];
	msgHolder[6] = txdataptr->data[6];
	msgHolder[7] = txdataptr->data[7];
	cmd = msgId;
	CAN.mailbox_set_datah(1,  msgHolder[4] + (msgHolder[5] << 8) + (msgHolder[6] << 16) + (msgHolder[7]<<24) );
	CAN.mailbox_set_datal(1, msgHolder[0] + (msgHolder[1] << 8) + (msgHolder[2] << 16) + (msgHolder[3]<<24) );
	CAN.mailbox_set_datalen(1,8);
	CAN.enable_interrupt(CAN_IER_MB1);
	CAN.enable_interrupt(CAN_IER_MB0); 
	CAN.global_send_transfer_cmd(CAN_TCR_MB1); // sends command
	//indexCnt = 0;
	dataptr = canRcvDataArray;
	for (ctrid=0; ctrid<numOfnodes; ctrid++){
		if (CanCheckTimeout(timeout) == true){                                // If no data, print timeout for node id
				Serial.print("-- -- -- Timeout broadcast after receiving "); Serial.print(ctrid); Serial.println(" messages");
				
				dataptr->id = 0;				// para di maisama sa ipaparse na data
				dataptr->data[0] = 0;	dataptr->data[1] = 0;
				dataptr->data[2] = 0;	dataptr->data[3] = 0;
				dataptr->data[4] = 0;	dataptr->data[5] = 0;
				dataptr->data[6] = 0;	dataptr->data[7] = 0;
				
				if (PRINT_MODE == 1){printRX_Frame(canRcvDataArray,ctrid,1);}
				//Serial1.print("ARQWAIT");
				Turn_off_column();
				CAN.disable();				        // reset can controller	
				error = 4;
				return error;
		}
		CAN.get_rx_buff(&incoming);
		if (incoming.data[0] != msgId){
				Serial.println("  !@#$%^ illegal msgId FOUND!!");
				Serial.print("id: "); Serial.print(incoming.id);
				Serial.print("  d0: "); Serial.println(incoming.data[0]);
				ctrid--;
				illegal_limit++; 
				if ( illegal_limit >= 25 ){ // consider this as repeating frame and resend cmd.
						Turn_off_column();
						CAN.disable();				        // reset can controller	
						dataptr->id = 0;				// para di maisama sa ipaparse na data
						dataptr->data[0] = 0;		dataptr->data[1] = 0;
						dataptr->data[2] = 0;		dataptr->data[3] = 0;
						dataptr->data[4] = 0;		dataptr->data[5] = 0;
						dataptr->data[6] = 0;		dataptr->data[7] = 0;
						error = 3;
						return error;
				}
				continue;
		}
		
		//check for repeating data here, ctrid-- medyo di gumagana.
		if (first_frame == 1) {
				first_frame++;
		} else {  
				c_dataptr = canRcvDataArray;
				//tid = incoming.id;              // compare node unique id
				//t0 = incoming.data[0];          // compare msgId
				for (int j=0; j <=numOfNodes; j++) {
					if (c_dataptr->id == incoming.id && c_dataptr->data[0] == incoming.data[0]){
						if (PRINT_MODE == 1) {Serial.print("    REPEATING :"); Serial.println(dataptr->id);}
						repeating = 1;
						break;
					}
					c_dataptr++;
				}
		}
		if (repeating == 1) {
				if (PRINT_MODE == 1) {Serial.println("  -- function: Broadcast_CMD() -- called -- repeating frames found"); }
				r_f_count++;
				Serial.print("  -- r_f_count : "); Serial.println(r_f_count); 
				if (r_f_count == REPEATING_FRAMES_LIMIT){    
						//Serial.print("  -- Repeating frames exceeded max count of "); Serial.println(REPEATING_FRAMES_LIMIT);
						//Serial1.print("ARQWAIT");		      // causes arq to not timeout waiting for serial
						r_f_count = 0;
						Turn_off_column();
						CAN.disable();				        // reset can controller	
						dataptr->id = 0;				// para di maisama sa ipaparse na data
						dataptr->data[0] = 0;		dataptr->data[1] = 0;
						dataptr->data[2] = 0;		dataptr->data[3] = 0;
						dataptr->data[4] = 0;		dataptr->data[5] = 0;
						dataptr->data[6] = 0;		dataptr->data[7] = 0;
						error = 3;
						return error;
				}
				ctrid--;      
				repeating = 0; 
		} else  {
                    // not repeating, place in struct dataptr
				store_can_frame(incoming.id,incoming.data[0],incoming.data[1],incoming.data[2],incoming.data[3],incoming.data[4],incoming.data[5],incoming.data[6],incoming.data[7]);
				if ( CanGetRcvArraySize(temp_can_rcv_data_array) == numOfNodes ){ //kumpleto na yung frames
						Serial.println("  Frames complete! :) ");
						CAN.disable();
						return 0;
				}
				dataptr->id = incoming.id;
				dataptr->dlc = incoming.dlc;
				dataptr->data[0]=incoming.data[0];		dataptr->data[1]=incoming.data[1];
				dataptr->data[2]=incoming.data[2];		dataptr->data[3]=incoming.data[3];
				dataptr->data[4]=incoming.data[4];		dataptr->data[5]=incoming.data[5];
				dataptr->data[6]=incoming.data[6];		dataptr->data[7]=incoming.data[7];
				dataptr++;
				} 
        }
	CAN.disable();
	error = 0;
	return error;
}

int store_can_frame(int id,int d0,int d1,int d2,int d3,int d4,int d5,int d6,int d7){
	RX_CAN_FRAME *check_ptr;
	unsigned long msgHolder[8];
	int unique = 1;
	if (PRINT_MODE == 1) {Serial.println("  -- function: store_can_frame() -- called");}
	if (prev_cmd == d0) { // meaning cmd was resent to the nodes
		Serial.print("    Storing frame with ID: "); Serial.println(id);
		//pwede na ifilter
		check_ptr = temp_can_rcv_data_array;
		for (int i = 0; i<numOfNodes;i++){
				if (check_ptr->id == id && check_ptr->data[0] == d0){
						if (PRINT_MODE == 1) {Serial.println("    FRAME already in temp_can_rcv_array.");}
						unique = 0;
						break;
				}
				check_ptr++;
		}
		if (unique == 1){
				//if (PRINT_MODE == 1) { Serial.println("    **FRAME stored.");}
				t_dataptr->id =id;
				t_dataptr->data[0]=d0;	t_dataptr->data[1]=d1;
				t_dataptr->data[2]=d2;	t_dataptr->data[3]=d3;
				t_dataptr->data[4]=d4;	t_dataptr->data[5]=d5;
				t_dataptr->data[6]=d6;	t_dataptr->data[7]=d7;
				//if (PRINT_MODE == 1) {printRX_Frame(temp_can_rcv_data_array,CanGetRcvArraySize(temp_can_rcv_data_array),1);}
				t_dataptr++;
		}
	} else if (prev_cmd != d0) {
				if (PRINT_MODE == 1) { Serial.println("    Command Changed ");}
				prev_cmd = d0;
				clear_can_array(temp_can_rcv_data_array);
				t_dataptr = temp_can_rcv_data_array; // START FROM BEGINNING OF TEMP_CAN_RCV_ARRAY
				//Serial.println("    **INTIAL FRAME stored.");
				t_dataptr->id =id;
				t_dataptr->data[0]=d0;	t_dataptr->data[1]=d1;
				t_dataptr->data[2]=d2;	t_dataptr->data[3]=d3;
				t_dataptr->data[4]=d4;	t_dataptr->data[5]=d5;
				t_dataptr->data[6]=d6;	t_dataptr->data[7]=d7;
				//if (PRINT_MODE == 1) {printRX_Frame(temp_can_rcv_data_array,CanGetRcvArraySize(temp_can_rcv_data_array),1);}
				t_dataptr++;
	}
 
}

void printRX_Frame(RX_CAN_FRAME* canRcvDataArray,int ctrid, int checksum){
    int sum = 0;
    char temp[10];
    RX_CAN_FRAME *dataptr;
    dataptr = canRcvDataArray;
    for(int x=0;x<ctrid;x++){	 
		sprintf(temp,"%02X",dataptr->id);Serial.print(temp);Serial.print("_");	 
		sprintf(temp,"%02X",dataptr->data[0]);Serial.print(temp);Serial.print("_");
		sprintf(temp,"%02X",dataptr->data[1]);Serial.print(temp);Serial.print("_");
		sprintf(temp,"%02X",dataptr->data[2]);Serial.print(temp);Serial.print("_");
		sprintf(temp,"%02X",dataptr->data[3]);Serial.print(temp);Serial.print("_");
		sprintf(temp,"%02X",dataptr->data[4]);Serial.print(temp);Serial.print("_");
		sprintf(temp,"%02X",dataptr->data[5]);Serial.print(temp);Serial.print("_");
		sprintf(temp,"%02X",dataptr->data[6]);Serial.print(temp);Serial.print("_");
		sprintf(temp,"%02X",dataptr->data[7]);Serial.print(temp);Serial.print("_");
		Serial.println("");
		dataptr++;
		if (checksum == 0){
			if(dataptr->id == 0)
			break;
		} else if( checksum == 1){
			sum = dataptr->id+dataptr->data[0]+dataptr->data[1]+dataptr->data[2]+dataptr->data[3]+dataptr->data[4]+dataptr->data[5]+dataptr->data[6]+dataptr->data[7];
			if (sum == 0){
			   break;
			}  
		}
    }
    Serial.println("");
}

void clear_uid(){
   if (PRINT_MODE == 1) {Serial.println("  -- function: clear_uid() -- called"); }
   for (int i = 0; i<numOfNodes ; i++ ){
      unique_ids[i] = 0;
   } 
}


 /**

 @brief        initializes the CAN module mailbox0 and mailbox1
 
 This function configures the CAN module of the controller to receive interrupts and assumes extended identifier
 for its output and input.
 The module is configured to use two mailboxes mailbox 1 is used as a transmitter mailbox and mailbox 0 is used 
 as a receiver mailbox. Interrupts are used to toggle flags.
  
  
 
@param [out]	can_data_array			initializes the container for future transactions
@param [in]		canbitrate				sets the bitrate of the CAN communication
@param [in]		numofnodes					Sets the buffer size of the receive buffer

@code
#define numberofnodes	40
#define canbitrate		40000

RX_CAN_FRAME can_rcv_data_array[numberofnodes];              //declare receive array
setup
{
	CanInitialize(canbitrate,can_rcv_data_array,numberofnodes);	
}
@endcode

 */
/* OLD SITES */
void can_initialize(void){
  //initialization of version 1 column
  if (CAN.init(SystemCoreClock, 40000)) {
    CAN.disable_interrupt(CAN_DISABLE_ALL_INTERRUPT_MASK);      // Disable all CAN0 interrupt
    NVIC_EnableIRQ(CAN0_IRQn);
    if (SERIAL)
    Serial.println("CAN Initialization SUCCESS\n\n");
  }
  else {
    if (SERIAL)
    Serial.println("CAN Initialization ERROR\n");
  }

  CAN.mailbox_set_mode(0, CAN_MB_RX_MODE);                     // Set MB0 as receiver
  CAN.mailbox_set_mode(1, CAN_MB_TX_MODE);                     // Set MB1 as transmitter
  CAN.mailbox_set_accept_mask(1, 0, false);                    // Accept mask filter of MB1
  CAN.mailbox_set_datalen(1, 8);                               // CAN frame data length

}
void check_timeout(void) {
  
  timeout_status = false; 
  int timestart = millis();
  
  do {
    if (millis() - timestart == TIMEOUT) {
      timeout_status = true;
      break;
    }
  } while (!CAN.rx_avail());                                   // while no frame received
  
}

/* NEW SITES VERSION2 */
void CanInitialize(unsigned long canbitrate, RX_CAN_FRAME* can_data_array,unsigned int numofnodes)
{
	unsigned int x,y;
	//canRcvArrySize = numofnodes;
	
	if (CAN.init(SystemCoreClock, canbitrate)) {
		CAN.disable_interrupt(CAN_DISABLE_ALL_INTERRUPT_MASK);			// Disable all CAN0 interrupt
		NVIC_EnableIRQ(CAN0_IRQn);
		if (SERIAL)
			Serial.println("CAN Initialization SUCCESS\n\n");
	}else 
	{
		if (SERIAL)
			Serial.println("CAN Initialization ERROR\n");
	}

	CAN.mailbox_set_mode(0, CAN_MB_RX_MODE);							// Set MB0 as receiver
	CAN.mailbox_set_id(0, 0, true);										// Set MB0 receive ID extended id
	CAN.mailbox_set_accept_mask(0,0,true);								//make it receive everything seen in bus
  
	CAN.mailbox_set_mode(1, CAN_MB_TX_MODE);							// Set MB1 as transmitter
	CAN.mailbox_set_id(1,MASTERMSGID, true);							// Set MB1 transfer ID to 1 extended id
	CAN.enable();
  
	//initialize the data array
	for(x=0;x<numofnodes;x++){
		can_data_array[x].id = 0;										//zero out the id
		can_data_array[x].dlc =0;
		for(y=0;y<8;y++) {
			can_data_array[x].data[y] = 0;								//zero out the databytes
		}
		
	}  
	return;
}


/**

@brief      waits for the receive buffer timeout to have any data
	
This function waits for the interrupt from the receive buffer. The time when it
 enters this function to the current time is compared if it excedes the timeout parameter
it exits and returns a true which means that the timeout occurred first.

@param [in] timeout		number of miliseconds before timeout would occur


@retval true    timeout occurred
@retval false    received a data

*/
bool CanCheckTimeout(unsigned long timeout) {
  bool ctimeout_status;
  ctimeout_status = false; 
  int timestart = millis();
  
  do {
    if (millis() - timestart == timeout) {
      ctimeout_status = true;
      break;
    }
  } while (!CAN.rx_avail());                                   // while no frame received
  
  return ctimeout_status;
}

/**

@brief      gets data from the can buffer
	
This function gets data from the can buffer. It tries to get data and is limited only by the current contents
of the CAN buffer and the maximum number of data of the receiving array. This gets the data from the
can buffer so that it would not be over written if not taken from the buffer

@param [out] rcv_data_array		number of miliseconds before timeout would occur
@param [in]	 maxnumofdata		maximum amount that would be gotten from the buffer



@returns	number of data taken


*/
int ArrangeCanMsgOrder(RX_CAN_FRAME* rcvDataArray,char order)
{
	//get number of messages in array using the last data
	//with message id of 0
	if (PRINT_MODE == 1) {Serial.println("  -- function called: ArrangeCanMsgOrder()"); }
	int msgnum;
	int x,y;
	RX_CAN_FRAME* dataptr;
	RX_CAN_FRAME temp;
	int index_of_min;
	int index_of_max;
	unsigned int id1,id2;
	
	dataptr = rcvDataArray;
	msgnum = 0;
	//while(dataptr->id != 0){
        while(dataptr->data[0] != 0){
		dataptr++;
		msgnum++;
		//error checking if there is no zero in id 
		if(msgnum > 1000){ //too large impossible to happen error
			return -1;	
		}
	}
	dataptr = rcvDataArray; //effectively point to rcvDataArray[0]
	if(!order){ //order smallest to largest
		for( x= 0; x< msgnum; x++){
			index_of_min = x;
			for(y=x;y<msgnum;y++){
				//c0de so that the ids could be arranged by geographic location
				if(!(getGIDfromUID((long int*)&dataptr[index_of_min].id) && getGIDfromUID((long int*)&dataptr[y].id))){//not found in table
						if(dataptr[index_of_min].id > dataptr[y].id){
							index_of_min = y;
						}
				}else{  //found in table
					id1 = getGIDfromUID((long int*)&dataptr[index_of_min].id);
					id2 = getGIDfromUID((long int*)&dataptr[y].id);
					if(id1>id2){
						index_of_min = y;
					}
				}
			}
			temp = dataptr[x];
			dataptr[x] = dataptr[index_of_min] ;
			dataptr[index_of_min] = temp;
		}	
	}else{ //sorting from largest to smallest
		for( x= 0; x< msgnum; x++){
			index_of_max = x;
			for(y=x;y<msgnum;y++){
				//c0de so that the ids could be arranged by geographic location
				if(!(getGIDfromUID((long int*)&dataptr[index_of_max].id)  && getGIDfromUID((long int*)&dataptr[y].id))){//not found in table
					if(dataptr[index_of_max].id < dataptr[y].id){
						index_of_max = y;
					}
				}else{  //found in table
					id1 = getGIDfromUID((long int*)&dataptr[index_of_max].id);
					id2 = getGIDfromUID((long int*)&dataptr[y].id);
					if(id1<id2){
						index_of_max = y;
					}
				}
			}
			temp = dataptr[x];
			dataptr[x] = dataptr[index_of_max] ;
			dataptr[index_of_max] = temp;
		}
	}
	return msgnum;
}

/**

@brief     This function prints data to serial assuming that the SERIAL global function is set


@param [in]	 canRcvDataArray	array containing can messages to be printed.
@param [in]	 numOfData			number of data to be printed from the array




@code
int numOfData;  //number of data received

numOfData = ArrangeCanMsgOrder(can_rcv_data_array,0); //arrange the data from lowest to highest
printCanData(can_rcv_data_array,numOfData);			  //print the data to serial

@endcode

*/		
void printCanData(RX_CAN_FRAME* canRcvDataArray,unsigned int numOfData)
{  
	if (PRINT_MODE == 1) {Serial.println("  -- function called: printCanData()"); }	
	RX_CAN_FRAME* dataptr;
	int ctrid;
	int x;
	ctrid = numOfData;
	if(numOfData < 1){ //error do nothing
		return;
	}
	 if(SERIAL){
		 char temp[10];
		 int count;
		 
		 Serial.println("CAN messages received ");
		 dataptr = canRcvDataArray;//point again at the start
		 for(x=0;x<ctrid;x++)
		 {	 
			 sprintf(temp,"%02X",dataptr->id); //CHANGED TO SHOW DECIMAL
			 Serial.print(temp);
			 Serial.print("_");
			 
			 sprintf(temp,"%02X",dataptr->data[0]);
			 Serial.print(temp);
			 Serial.print("_");
			 
			 sprintf(temp,"%02X",dataptr->data[1]);
			 Serial.print(temp);
			 Serial.print("_");
			 
			 sprintf(temp,"%02X",dataptr->data[2]);
			 Serial.print(temp);
			 Serial.print("_");
			 
			 sprintf(temp,"%02X",dataptr->data[3]);
			 Serial.print(temp);
			 Serial.print("_");
			 
			 sprintf(temp,"%02X",dataptr->data[4]);
			 Serial.print(temp);
			 Serial.print("_");
			 
			 sprintf(temp,"%02X",dataptr->data[5]);
			 Serial.print(temp);
			 Serial.print("_");
			 
			 sprintf(temp,"%02X",dataptr->data[6]);
			 Serial.print(temp);
			 Serial.print("_");
			 
			 sprintf(temp,"%02X",dataptr->data[7]);
			 Serial.print(temp);
			 Serial.print("_");

			 Serial.println("");
			 dataptr++;

			//if(dataptr->id == 0)
                        if (dataptr->data[0] == 0)
				break; 
		 }
	 }				
}

/**

@brief     This function checks how many CAN messages in the array



@param [in]	 canRcvDataArray	array containing can messages to be printed.



@note
This relies on the fact that a dummy data at the end contains an id of 0

@code

MasterBroadcast(ptr,can_rcv_data_array,numberofnodes,timeout);

//at this point the data is in can_rcv_data_array
//to get all of the data we need its size
size = CanGetRcvArraySize(can_rcv_data_array);

//to print out the id of all of the messages
ptr = can_rcv_data_array;
for(x =0;x< size;x++){
	
	sprintf(temp,"%02X",ptr->id);
	Serial.print(temp);
	ptr++;
}

@endcode

*/
unsigned int CanGetRcvArraySize(RX_CAN_FRAME* rcvDataArray){
	int msgnum,checksum;
	RX_CAN_FRAME* dataptr;
	dataptr = rcvDataArray;
	msgnum = 0;
        checksum = 1; // force code to enter loop
        while(dataptr->id+dataptr->data[0]!= 0){
		dataptr++;
		msgnum++;
		/*error checking if there is no zero in id */
		if(msgnum > 1000){ //too large impossible to happen error
			return -1;
		}
	}
	return msgnum;	
}

/**

@brief      creates the table in the RAM from the values listed in the SDCARD
	
Reads an CSV from the SD card and places the data there to the table

*/
void generateGIDtable(void){
	if (PRINT_MODE == 1) {Serial.println("function: generateGIDtable() -- called"); }
	 for(int count = 0;count < CANARRAYMAXSIZE;count++){
		GIDTable[count][0] = count+1;
	 }	 
}
 
void generateSOMSspecificTable(void){
   	if (PRINT_MODE == 1) {Serial.println("function: generateSOMSspecificTable() -- called"); }
	  for(int count = 0;count < CANARRAYMAXSIZE;count++){
		  SOMSNodeSpecificTable[count][0] = count+1;
	 }
}
 
 /**

@brief      gets the map of the Geographic ID from the Unique id
	
This function reads from the ram the geographic id given the unique id of 
the node. This assumes that the table is already filled

@param [in] uid			the unique id of the node


@returns	the geographic id or zero


*/
 unsigned int getGIDfromUID( long *uid){
	 for(int count = 0;count < CANARRAYMAXSIZE;count++){
		 if(GIDTable[count][1] == *uid)
			return GIDTable[count][0];
	 }
	 return 0;
 }
 

/**

@brief     this function converts the message from the CAN bus and translates it to a character array




@param [in]	 data	one set of data consisting of id/d1/d2/d3/d4/d5/d6/d7/d8
@param [in]	 whole_message	The character array/string where the converted string would be appended to




@code
int x;
RX_CAN_FRAME *dataptr;
dataptr = data;
for(x=0;x<sizeofarray;x++)
{
	append_data_to_message(dataptr,whole_message);
	dataptr++;
}
@endcode

*/
int append_data_to_message(RX_CAN_FRAME *data,char*whole_message){
	  
		char templine[25];
		char temp[5];
		int long idread;
		RX_CAN_FRAME *dataptr;
		int sdvalue;
		sdvalue = 0;
		dataptr = data;
	  
	  	if(!sdvalue){ //sdcard load successfull
			idread = dataptr->id;
			if(!getGIDfromUID(&idread))
				sprintf(temp,"%04X",dataptr->id);//not found in table
			else
	  			sprintf(temp,"%04X",getGIDfromUID(&idread));
			strcat(whole_message,temp);  
	  	}else{
	  		sprintf(temp,"%04X",dataptr->id);
			strcat(whole_message,temp);  
		}
	  
	  	
	  	sprintf(temp,"%02X",dataptr->data[0]);
		strcat(whole_message,temp);
	  	
	  	sprintf(temp,"%02X",dataptr->data[1]);
		strcat(whole_message,temp);
	  	
	  	sprintf(temp,"%02X",dataptr->data[2]);
		strcat(whole_message,temp);
	  	
	  	sprintf(temp,"%02X",dataptr->data[3]);
		strcat(whole_message,temp);
		
	  	sprintf(temp,"%02X",dataptr->data[4]);
		strcat(whole_message,temp);
	  	
	  	sprintf(temp,"%02X",dataptr->data[5]);
		strcat(whole_message,temp);
	  	
	  	sprintf(temp,"%02X",dataptr->data[6]);
		strcat(whole_message,temp);
	  	
	  	sprintf(temp,"%02X",dataptr->data[7]);
		strcat(whole_message,temp);
	  
	  return 0;	  
}
  
  
/**

@brief     this function converts an array of CAN data into an array of characters



@param [in]	 data	one array of data consisting of id/d1/d2/d3/d4/d5/d6/d7/d8
@param [in]	 whole_message	The character array/string where the converted string would be appended to
@parama[in]	 sizeofarray	The size of array



@code

@endcode

*/
void append_array_to_message(RX_CAN_FRAME *data,char*whole_message,unsigned int sizeofarray){
		int x;
		RX_CAN_FRAME *dataptr;
                if(PRINT_MODE == 1) {printCanData(data,sizeofarray);}	
		dataptr = data;
	  	for(x=0;x<sizeofarray;x++){
		  	append_data_to_message(dataptr,whole_message);
		  	dataptr++;
	  	}  
	  	return;
}
 
void append_to_end_of_can_array(RX_CAN_FRAME *dest,RX_CAN_FRAME *src){
	   unsigned int sizeofarray;
	   RX_CAN_FRAME *canptr;
	   sizeofarray = CanGetRcvArraySize(dest);
	   canptr = dest;
	   canptr += sizeofarray;
	   *canptr = *src;
	   canptr++;
	   canptr->id = 0; //delimeter for end
           canptr->data[0] = 0;
}
  
void clear_can_array(RX_CAN_FRAME *can_data_array){
	for(int x=0;x<numOfNodes;x++){
		can_data_array[x].id = 0;										//zero out the id
		can_data_array[x].dlc =0;
		for(int y=0;y<8;y++) {
			can_data_array[x].data[y] = 0;								//zero out the databytes
		}
		
	} 
}

void Turn_on_column(){
	if (PRINT_MODE == 1) {Serial.println("     function: Turn_on_column() -- called"); }
	digitalWrite(RELAYPIN,HIGH);
	delay(TURN_ON_DELAY); 
}

void Turn_off_column(){
	if (PRINT_MODE == 1) {Serial.println("     function: Turn_off_column() -- called"); }
        if (TURN_ON_DELAY + COLUMN_COOL_OFF > 10000){
            Serial.print("ARQWAIT.");
            Serial1.println("ARQWAIT"); 
        }
	digitalWrite(RELAYPIN, LOW);
	delay(COLUMN_COOL_OFF);
}
