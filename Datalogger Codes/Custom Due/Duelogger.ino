
/**
 * @file Duelogger.ino
 * @author K Razon, J Serrato, R Canseco
 * @date 4 Dec 2015
 * @brief Main file for the code of Due based datalogger for the SENSLOPE PROJECT.
 *
 * Here typically goes a more extensive explanation of what the header
 * defines. Doxygens tags are words preceeded by either a backslash @\
 * or by an at symbol @@.
 * 
 * 
 */
//constants
#define BUILDNUMBER "REV_28"
#define BAUD 115200
#define BAUDARQ 9600
//#define TIMEOUT 1500
#define VERSION 3


//TURN ON PIN 25
#define NUMBERNODES 40
#define CUSTOMDUE true


// Required libraries
#include "variant.h"
#include <due_can.h>
#include <DueFlashStorage.h>
DueFlashStorage dueFlashStorage;


int PASS_AXEL1_ADC_CALIB_MINMAX = 32;        // version 2
int PASS_AXEL2_ADC_CALIB_MINMAX = 33;
int POLL_SOMS_RAW_NEW  =111;
int POLL_SOMS_CALIB_NEW = 112;

int sensorVersion = 3;
int dataloggerVersion = 2;
/*
#define PASS_AXEL1_ADC_CALIB_MINMAX         11          // version 3
#define PASS_AXEL2_ADC_CALIB_MINMAX         12
#define POLL_SOMS_RAW_NEW                  110
#define POLL_SOMS_CALIB_NEW                113
*/
int COLUMN_COOL_OFF = 2000;

int TIMEOUT = 3000;
int unique_ids[40] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
int t_unique_ids[40] = {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1}; // temp unique node ids

RX_CAN_FRAME incoming;
int inbyte, id, cmd;
int columnLen, loopnum, commandCAN;
int nodearray[NUMBERNODES] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40};
unsigned long GIDTable[100][2];
bool timeout_status = false;

char MASTERNAME[6] = "XXXXX";
char ColumnCommand = 'T'; 
unsigned int numOfNodes = 11;
char TIMESTAMP[19];

int lastSec, lastMin, lastHr;
char columnData[2500];
char allData[5000];
char subcolumnData[150];
char subcolumnDataBackUp[150];
char *parsedData= {};

/*can stuff */
#define numberofnodes	100 
RX_CAN_FRAME can_rcv_data_array[numberofnodes];
RX_CAN_FRAME temp_can_rcv_data_array[numberofnodes];
TX_CAN_FRAME can_snd_data_array[2];

/*from mega global variables*/
const int trigSW =  50;

/*SD card variables */
/*config file variables  with default values*/
 int PRINT_MODE = 0;
 int REPEATING_FRAMES_LIMIT = 10; // how many repeating frames before the column power is reset
 unsigned int NO_COLUMN_LIMIT = 2; // kapag kulang yung responding nodes or wala talaga, ilang retries bago tumuloy yung program
 unsigned int REPEATING_FRAMES_RETRY_LIMIT = 2; 
 int TURN_ON_DELAY = 2000;

unsigned int column1;

int i=0;		
char tempo[3];
char line[40];
int numOfData,sizeofarray,msgid;
unsigned int counter;

extern RX_CAN_FRAME can_default_buffer[100];	
int RELAYPIN =44;  //CUSTOM DUE
int chipSelect = SS3;  //!< the pin for chip select in sdcard
bool eepromFlag = true;
 
void setup() {
    
    RELAYPIN =44;  //CUSTOM DUE
    chipSelect = SS3;  //!< the pin for chip select in sdcard

    pinMode(51, OUTPUT);
    digitalWrite(51, HIGH);
    
    int debugCheck=0;
    pinMode(trigSW, OUTPUT);
    digitalWrite(trigSW, LOW);
	
    pinMode(RELAYPIN, OUTPUT);
    digitalWrite(RELAYPIN, LOW);
    delay(1000);
    
    Serial.begin(BAUD);
    Serial1.begin(BAUDARQ);

    Serial.print("\nSENSLOPE ");
    Serial.print(MASTERNAME);
    Serial.println(" MASTER BOX");
    Serial.print("Build no:  ");
    Serial.println(BUILDNUMBER);

    
    if (sensorVersion == 2){
      PASS_AXEL1_ADC_CALIB_MINMAX = 32;       
      PASS_AXEL2_ADC_CALIB_MINMAX = 33;        
      POLL_SOMS_RAW_NEW = 111;                 
      POLL_SOMS_CALIB_NEW = 112;           
  } else if ( sensorVersion == 3){
      PASS_AXEL1_ADC_CALIB_MINMAX = 11;
      PASS_AXEL2_ADC_CALIB_MINMAX = 12;      
      POLL_SOMS_RAW_NEW = 110;                 
      POLL_SOMS_CALIB_NEW = 113;                      
  } else { // set default as version 3
      PASS_AXEL1_ADC_CALIB_MINMAX = 11;
      PASS_AXEL2_ADC_CALIB_MINMAX = 12;      
      POLL_SOMS_RAW_NEW = 110;                 
      POLL_SOMS_CALIB_NEW = 113;  
  }
  
    int timestart = millis();
    int timenow = millis();  
    generateGIDtable();			//initialize the table in the ram
    
    
  Serial.println("Press anything in 3 seconds to copy config file from sd card");
  while ( timenow - timestart < 3000 ) {
    timenow = millis();
    if (Serial.available()) {
      Serial.println("Changed the EEPROM flag to TRUE");
      eepromFlag= true;
      break;
    }
  }
  if (eepromFlag == true) {
    loadVariablesFromSdcard();
  }
   
  if (numOfNodes != 255){
    for (int i = 0; i < numOfNodes; i++) {
    t_unique_ids[i] = GIDTable[i][1];
  }
  }

   getValuesfromEEPROM();

    Serial.println("Press anything to enter debugger mode");
	while( timenow - timestart < 5000 ){
		timenow = millis();
		if (Serial.available()){
			Serial.println("Waiting for commands... DBGCMD9 TO EXIT");
			while(debugCheck==0){
				menu(false);         
				delay(1000);
			}
		}    
	}
    Serial.println("Continuing...");
    Serial.println(" ");
}

void loop() {
	menu(true);			
	delay(200);
}


void menu (bool arq){
    int mode = 0;
    char cmd1;
    char cmd2;
    char serbuffer[22]= {};

    if (arq == false){
        mode= 0;        
    } else{
        mode=1;
    }

    if (arq){
        if (CUSTOMDUE){        
			while (!Serial1.available());
			Serial1.readBytesUntil('\n',serbuffer,22);
		} else{     
			while (!Serial3.available());
			Serial3.readBytesUntil('\n',serbuffer,22);
		}
	} else{
		while (!Serial.available());
		Serial.readBytesUntil('\n',serbuffer,22);
    }
    serbuffer[22]=0x00;
    Serial.println(serbuffer);    
    if (strstr(serbuffer,"CMD")){
		cmd1 = serbuffer[6];
		cmd2 = serbuffer[7];
        //FOR
        for (i = 8; i < 22; i++){ 
            ;
        TIMESTAMP[i-8]= serbuffer[i];
        }
        Serial.print("TIMESTAMP: ");
        Serial.println(TIMESTAMP);
    }
            
    switch(cmd1){
    //debugcheck
       
        case '6' :{
            Serial.print("Recognized ARQCMD6- GET BROADCASTEDNEW DATA");
            getdataBroadcastNew(mode, cmd2);
            break;
        }
       case '9':{
            Serial.println("Exiting debug mode");
            //debugCheck=1;
       }
        default:{
           Serial.println("Command not recognized");
           break;
       }
    }//SWITCH
}
void printData(char *rawcolData, int mode ) {
  int cmd = 0;
  char idfier[5] = "";
  int cutoff[5] = {};
  char lenc[2] = {};
  String strarray[5] = {}; // String array
  const char s[2] = "+";
  char tokenlen[1000] = "";
  int subloopnum[5] = {};
  char *token;
  
  /* get the first token */
  token = strtok(rawcolData, s);
    
  int j = 0;

  while (token != NULL) {
    strarray[j] = token;
    token = strtok(NULL, s);
    delay(200);
    j++;
  }

  int columnLen = 0, loopnum = 0;
  
  //create another function here: check identifier

  for (i = 0; i < j; i++) {
    
    strarray[i].toCharArray(tokenlen,999);
    Serial.println(strarray[i]);
    columnLen = strlen(tokenlen);

    //check yung message id 

    switch (tokenlen[2]) {
       case '1': {
        if (tokenlen[3]== '5') {
          idfier[i]='b';
          cutoff[i] = 130;
        }
        else if (tokenlen[3]== 'A') {
          idfier[i]='c';
          cutoff[i] = 133;    
        }
        break;
      }
      
      case '0': {
        cutoff[i] = 135;  //15 chars only for axel
        if (tokenlen[3]== 'B')
          idfier[i]='x';
        else if (tokenlen[3]== 'C')
          idfier[i]='y';      
        break;
      }
      case '2': {
        cutoff[i] = 135;  //15 chars only for axel
        if (tokenlen[3]== '0')
          idfier[i]='x';
        else if (tokenlen[3]== '1')
          idfier[i]='y';      
        break;
      }
      case '6': {
        idfier[i]='b';
        cutoff[i] = 130;  //10 chars only for axel
        break;
      }  
      case '7': {
        idfier[i]='c';
        cutoff[i] = 133;  //7 chars for raw soms
        break;
      }
      default: {
        idfier[i]= '0';
        cutoff[i] = 0;
        break;
        }
    }
   
    if (columnLen == 0){
      subloopnum[i] = 0;
    } else {
      subloopnum[i] = (columnLen/ cutoff[i]);
      if (columnLen%cutoff[i] != 0){
        subloopnum[i] ++;
      } 
    }
    loopnum = loopnum + subloopnum[i];
  }// END OF FOR LOO

  
  if (loopnum <= 0) {
    Serial.println("if (loopnum <= 0)");
    // if there is an error parsing the data or no data
    sprintf(subcolumnData, "040", 3);
    strncat(subcolumnData, ">>", 2);
    strncat(subcolumnData, "1/1#", 4);
    strncat(subcolumnData, MASTERNAME, 5);
    strncat(subcolumnData, "*0*ERROR: no data parsed<<", 26);
    if (mode == 1) { //arqMode
      //Serial3.println(subcolumnData);
      Serial1.println(subcolumnData);
      Serial.println(subcolumnData);
    }

    else if (mode == 0) { //debugMode
      Serial.println(subcolumnData);
    }
    Serial1.println(subcolumnData);
    Serial.println(subcolumnData);
    
  } else {
    //len is not yet fixed-----------------------------------
    int partnum = 0;  
    int subpartnum = 0;
      
    for (i = 0; i < j; i++) {
      char charnum[1] = {};
      int len = 0 ;
      int notState = 0;
      char *columnPointer = "";
      strarray[i].toCharArray(tokenlen,999);
      columnPointer= tokenlen;
      subpartnum = 0;  
      while (subpartnum < subloopnum[i]) {            // if data were present
        if (strlen(columnPointer) > cutoff[i]) {
          len = cutoff[i] + 20;
          if (len < 99 ) {
            sprintf(subcolumnData, "0", 1);
            sprintf(lenc, "%d", len);
            strncat(subcolumnData, lenc, 2);
          } else {
            sprintf(subcolumnData, "%d", len);
          }
        } else {
        len = strlen(columnPointer);
        len = len + 20;
        if (len < 99 ) {
          sprintf(subcolumnData, "0", 1);
          sprintf(lenc, "%d", len);
          strncat(subcolumnData, lenc, 2);
        } else {
          sprintf(subcolumnData, "%d", len);
        }
      }
      strncat(subcolumnData, ">>", 2);
      if (partnum > 8){
      sprintf(charnum, "%d", ((partnum + 1)/10));
      strncat(subcolumnData, charnum, 1);
      }
      sprintf(charnum, "%d", (partnum + 1)-(((partnum + 1)/10)*10));
      strncat(subcolumnData, charnum, 1);
      strncat(subcolumnData, "/", 1);
      if (loopnum > 8){
      sprintf(charnum, "%d", ((loopnum)/10));
      strncat(subcolumnData, charnum, 1);
      }
      sprintf(charnum, "%d", loopnum-(((loopnum)/10)*10));
      strncat(subcolumnData, charnum, 1);
      strncat(subcolumnData, "#", 1);
      strncat(subcolumnData, MASTERNAME, 5);
      strncat(subcolumnData, "*", 1);
      sprintf(charnum, "%c", idfier[i]);
      strncat(subcolumnData, charnum, 1);
      strncat(subcolumnData, "*", 1);
      strncat(subcolumnData, columnPointer, cutoff[i]);
      strncat(subcolumnData, "<<", 2);
      
      columnPointer = columnPointer + cutoff[i];
      if (partnum == 0) {
        Serial.println(subcolumnData);
        Serial1.println(subcolumnData);
        //subcolumnDataBackUp= subcolumnData;
        strcpy(subcolumnDataBackUp,subcolumnData);

        //Serial.println(subcolumnData);
        //Serial1.println(subcolumnData);
            
      }
      else if (partnum > 0) {
        Serial.print("partnum == "); Serial.println(partnum);

        if (mode == 0) { // debug
          int OKflag = 0;
          do {
            int timestart=millis();
            int timenow= millis();

            while (!Serial.available()){
              while( timenow - timestart < 9000 ){
                timenow = millis();
              }
              Serial.println("Time out...");
              break;
            }    
            if (Serial.find("OK")){
//              delay(9000);
              Serial.println("OK found");
              Serial.println(subcolumnData);
              OKflag = 1;
            }
            else{
              Serial.println(subcolumnDataBackUp);
              OKflag = 0; 
            }
          } while (OKflag != 1);
          
        }

        if (mode == 1) { // ARQ
          int OKflag = 0;
          do {
            int timestart=millis();
            int timenow= millis();

            while (!Serial1.available()){
              while( timenow - timestart < 9000 ){
                timenow = millis();
              }
              Serial.println("Time out...");
              break;
            }    
            if (Serial1.find("OK")){
//              delay(9000);
              Serial.println("OK found");
              Serial1.println(subcolumnData);
              OKflag = 1;
            }
            else{
              Serial1.println(subcolumnDataBackUp);
              OKflag = 0; 
            }
          } while (OKflag != 1);
        }
      }
      partnum = partnum + 1;
      subpartnum = subpartnum + 1;
      //subcolumnDataBackUp= subcolumnData;
      strcpy(subcolumnDataBackUp,subcolumnData);
    }
  }

 }
 delay(2000);
 Serial.println("done");
 Serial1.println("ARQSTOP");
}

char *parser(char *raw, int cmd){
  int i=0, datalength=0;
  char pArray[2500] = {};
  datalength  = strlen(raw);

  Serial.println("Acquiring data...");

  for (i = 0; i < datalength; i++, raw++){      
    switch(cmd){
      case 1: {// axel data //13    
        if (i%20 != 0 && i%20!= 1 && i%20 != 4 && i%20 != 5 && i%20 != 8 && i%20 != 12 && i%20 != 16 ){ strncat(pArray, raw, 1); }
        break;
      }
      case 2:{ // raw soms // 10
        if (i%20 != 0 && i%20!= 1 && i%20 != 8 && i%20 != 12 && i%20 < 14 ) {strncat(pArray, raw, 1); }
        break;
      }
    
      case 3:{ //calib soms //7
        //ginawang 14 muna 
        //if (i%20 != 0 && i%20!= 1 && i%20 != 4 && i%20!= 5 && i%20 < 16 ) { strncat(pArray, raw, 1); }
        if (i%20 != 0 && i%20!= 1 && i%20!= 8 && i%20 < 10 ) { strncat(pArray, raw, 1); }
        break;
      }
    
      case 4:{ // old format
        if (i%18 != 2 && i%18!= 6 && i%18 != 10) {strncat(pArray, raw, 1);}
        break;
      }
    
      case 8:{ // old axel /for 15
        if (i%20 != 0 && i%20!= 1 && i%20 != 8 && i%20 != 12 && i%20 != 16 ) {strncat(pArray, raw, 1);}
        break;
      }
    } 
  }
  pArray[i]= '\0';
  i=0;
  return pArray;
}

