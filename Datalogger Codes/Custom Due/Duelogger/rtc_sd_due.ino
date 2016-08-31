#include <SD.h>
#include <SPI.h>
#include <string.h>

File sdFile;			   //!< file for interfacing SdCard files

#define WAIT_TO_START    1 // Wait for serial input in setup()
#define SYNC_INTERVAL 1000 // mills between calls to sync()
const int  cs=18; //chip select RTC

/*rap modification due logger implementation*/
char loggerFileName[100];


/************************************************************************/
/* @brief      reads the sdcard and gets the configuration from the sdcard
 /************************************************************************/
 unsigned int loadVariablesFromSdcard(void){
	 char maxline  = 18;
	 char linecount = 0;
	 unsigned char endoffile;
	 unsigned int lineLength = 1000;
	 unsigned int x;
	 char oneLine[lineLength];
	 char *byteptr;

	initGIDTable();
	initiSOMSNodeSpecificTable();
		
	SPI.setBitOrder(LSBFIRST); 
		
	if (!SD.begin(6,chipSelect)) {
		   Serial.println(" SDCARD initialization failed!");
		   return -1;
	}
	  
	 //return if configuration file not found	 
	 sdFile = SD.open("CONFIG.txt", FILE_READ);
	 
        //read the part on the nodes and place them in the global variable
	 if(!sdFile){
		 Serial.println("failed to open file");
		 return -2;
	 }
	 Serial.println("Reading contents");	
	 byteptr = oneLine;
	 while((*byteptr = sdFile.peek())!= -1){
		 byteptr = oneLine;
		 while(((*byteptr = sdFile.read())!= '\n') ){
		 
			byteptr++; 
			
			if(*byteptr == -1)
				break;	
		 }
		 
		 linecount++;
		 if(linecount>maxline) break;
		
		 endoffile =processConfigLine(oneLine); //check if line has a command
		 if(endoffile) break;
			
		 //clearout oneLine
		 for(x=0;x<lineLength;x++){
			 oneLine[x] = 0;
		 }
		 
	 }
	 //read number of nodes
	 
	//read number of columns
	Serial.println("Finished Reading Configuration file");
	
        sdFile.close();
	SD.end();
	
        //make sure to disable the chip select pin
	digitalWrite(chipSelect, HIGH);
	return 0;
	 
 }
unsigned int processConfigLine(char *ptr){
	String str,str2;
   	char *sptr;
   	char buff[64];
   	int indexOfValue,value;
	   
    //check if matches any of the commands
	str = String(ptr);
	   
	if(str.startsWith("MasterName")){ //checks if the command is for column	
		indexOfValue =str.indexOf("=");
		indexOfValue++; // get index of the number
		while(str[indexOfValue] == ' ')
		indexOfValue++;
		sptr = &str[indexOfValue];
		strncpy(MASTERNAME,sptr,5);
		sprintf(buff,"New MasterName = ");
		strncat(buff,MASTERNAME,5);
		strcat(buff,"\n");
               
                dueFlashStorage.write(3, MASTERNAME[0]);
                dueFlashStorage.write(4, MASTERNAME[1]);
                dueFlashStorage.write(5, MASTERNAME[2]);
                dueFlashStorage.write(6, MASTERNAME[3]);
                dueFlashStorage.write(7, MASTERNAME[4]);
               
		//Serial.println(buff);
		return 0;
		
	}

	else if(str.startsWith("dataloggerVersion")){
	   	indexOfValue =str.indexOf("=");
	   	indexOfValue++; // get index of the number
	   	sptr = &str[indexOfValue];
	   	str2 = String(sptr);
	   	value = str2.toInt();
                dataloggerVersion = value;
	  // 	sprintf(buff,"numOfNodes Found. New value = %d",dataloggerVersion);
	   	//Serial.println(buff);
       	dueFlashStorage.write(15, value);
	  	return 0;
	}

	else if(str.startsWith("sensorVersion")){
	   	indexOfValue =str.indexOf("=");
	   	indexOfValue++; // get index of the number
	   	sptr = &str[indexOfValue];
	   	str2 = String(sptr);
	   	value = str2.toInt();
                sensorVersion = value;
	  // 	sprintf(buff,"numOfNodes Found. New value = %d",sensorVersion);
	   	//Serial.println(buff);
       	 dueFlashStorage.write(16, value);
	  	return 0;
	}

	else if(str.startsWith("numOfNodes")){
	   	indexOfValue =str.indexOf("=");
	   	indexOfValue++; // get index of the number
	   	sptr = &str[indexOfValue];
	   	str2 = String(sptr);
	   	value = str2.toInt();
	   	numOfNodes = value;
	   //	sprintf(buff,"numOfNodes Found. New value = %d",numOfNodes);
	   //	Serial.println(buff);
       	dueFlashStorage.write(0, numOfNodes);
	  	return 0;
	}

	else if(str.startsWith("columnIDs") || str.startsWith("column1")){
	   	indexOfValue =str.indexOf("=");
	   	indexOfValue= indexOfValue + 2; // get index of the number
	   	sptr = &str[indexOfValue];
		   	
	   	char *colptr;
	   	colptr = strtok(sptr,",");
	   	int x = 0;
		   	
	   	while((colptr != NULL)){
           	char colHI[3]= {};
                char colLOW[3]= {};
                    
		   	GIDTable[x][1] = atoi(colptr);
		   	if (GIDTable[x][1] > 999){
               	colHI= {colptr[0],colptr[1], '\0'};
                colLOW = {colptr[2],colptr[3], '\0'}; 
            }
            else{
              	colHI= {colptr[0], '\0'};
                colLOW= {colptr[1],colptr[2], '\0'};
            }
                           
           // sprintf(buff,"column1 Found. New value = %d",GIDTable[x][1]);
            //Serial.println(buff);
            dueFlashStorage.write(20 + (x*2), atoi(colHI));
            dueFlashStorage.write(21 + (x*2), atoi(colLOW));

		   	colptr = strtok(NULL,",");
		   	x++;
		}

		numOfNodes = x; // set number of nodes of column1
		str2 = String(sptr);
		value = str2.toInt();
		//sprintf(buff,"column1 Found. New value = %d",value);
		//Serial.println(buff);
		return 0;
      }
    

	else if(str.startsWith("ColumnCommand")){ //checks if the command is for column
		indexOfValue =str.indexOf("=");
	 	indexOfValue++; // get index of the number
	 	while(str[indexOfValue] == ' ')
		 	indexOfValue++;
		sptr = &str[indexOfValue];
	 	ColumnCommand = *sptr;
	 	//sprintf(buff,"ColumnCommand Found = %c",ColumnCommand);
	 	dueFlashStorage.write(1, ColumnCommand);
	 	//Serial.println(buff);
	 	return 0;		 
	}

	else if(str.startsWith("column_cool_off")){
		indexOfValue =str.indexOf("=");
		indexOfValue++; // get index of the number
	  	sptr = &str[indexOfValue];
	  	str2 = String(sptr);
	   	value = str2.toInt();
                COLUMN_COOL_OFF = value*100;
	   	//sprintf(buff,"TURN_ON_DELAY Found. New value = %d",TURN_ON_DELAY);
	   	//Serial.println(buff);
	   	dueFlashStorage.write(17, value);
        return 0;
                   	   
	}

	else if(str.startsWith("sampling_max_num_of_retry")){
		indexOfValue =str.indexOf("=");
		indexOfValue++; // get index of the number
		sptr = &str[indexOfValue];
		str2 = String(sptr);
		value = str2.toInt();
                NO_COLUMN_LIMIT = value;
		//sprintf(buff,"NO_COLUMN_LIMIT = %d",NO_COLUMN_LIMIT);
		dueFlashStorage.write(8, value);
        //Serial.println(buff);
		return 0;

	}

    else if(str.startsWith("turn_on_delay")){
		indexOfValue =str.indexOf("=");
		indexOfValue++; // get index of the number
	  	sptr = &str[indexOfValue];
	  	str2 = String(sptr);
	   	value = str2.toInt();
	   	TURN_ON_DELAY = value;
	   	//sprintf(buff,"TURN_ON_DELAY Found. New value = %d",TURN_ON_DELAY);
	   //	Serial.println(buff);
	   	dueFlashStorage.write(9, TURN_ON_DELAY);
                TURN_ON_DELAY = TURN_ON_DELAY* 100;
                return 0;
                   	   
	}
	
	else if(str.startsWith("repeating_frames_limit")){
		indexOfValue =str.indexOf("=");
		indexOfValue++; // get index of the number
		sptr = &str[indexOfValue];
		str2 = String(sptr);
		value = str2.toInt();
		REPEATING_FRAMES_LIMIT = value;
		//sprintf(buff,"REPEATING_FRAMES_LIMIT = %d",REPEATING_FRAMES_LIMIT);
		dueFlashStorage.write(11, REPEATING_FRAMES_LIMIT);
       // Serial.println(buff);
		return 0;

	}
	else if(str.startsWith("repeating_retry_limit")){
		indexOfValue =str.indexOf("=");
		indexOfValue++; // get index of the number
		sptr = &str[indexOfValue];
		str2 = String(sptr);
		value = str2.toInt();
		REPEATING_FRAMES_RETRY_LIMIT  = value;
		//sprintf(buff,"REPEATING_FRAMES_RETRY_LIMIT = %d",REPEATING_FRAMES_RETRY_LIMIT );
		dueFlashStorage.write(14, REPEATING_FRAMES_RETRY_LIMIT);
       // Serial.println(buff);
		return 0;

	}
        else if(str.startsWith("PIEZO")){
			indexOfValue =str.indexOf("=");
			indexOfValue++; // get index of the number
			sptr = &str[indexOfValue];
			str2 = String(sptr);
			value = str2.toInt();
			PIEZO = value;
			//sprintf(buff,"PIEZO = %d",PIEZO);
			//Serial.println(buff);
                        dueFlashStorage.write(18, PIEZO);
			return 0;
	}
	else if(str.startsWith("print_mode")){
		indexOfValue =str.indexOf("=");
		indexOfValue++; // get index of the number
		sptr = &str[indexOfValue];
		str2 = String(sptr);
		value = str2.toInt();
		PRINT_MODE = value;
		//sprintf(buff,"PRINT_MODE = %d",PRINT_MODE);
		dueFlashStorage.write(13, PRINT_MODE);
       // Serial.println(buff);
		return 0;

	}

	else if(str.startsWith("ENDOFCONFIG")){ //checks if the command is for column
		Serial.println("End of file found");
		return 1;
		
	}
	else{
		Serial.println("UnknownLine");
		return 0;
	}
  }

   
 
 void initGIDTable(void){
	  unsigned int count;
	  count = 0;
	  for(count = 0;count < CANARRAYMAXSIZE;count++){
		  GIDTable[count][1] = 0;
		  GIDTable[count][0] = count+1;
	  }
 }
 
 void initiSOMSNodeSpecificTable(void){
	 unsigned int count;
	 count = 0;
	 for(count = 0;count < CANARRAYMAXSIZE;count++){
		 SOMSNodeSpecificTable[count][1] = 0;
		 SOMSNodeSpecificTable[count][0] = count+1;
	 }
 }


//=====================================
String ReadTimeDate(int *secp, int *minp, int *hrp){
  String temp;
  int TimeDate [7]; //second,minute,hour,null,day,month,year   
  
   SPI.setBitOrder(MSBFIRST);
    SPI.setDataMode(SPI_MODE1); // both mode 1 & 3 should work   
  for(int i=0; i<=6;i++){
    if(i==3)
      i++;
    digitalWrite(cs, LOW);
    SPI.transfer(i+0x00);
	
	 
    unsigned int n = SPI.transfer(0x00);
	
	   
    digitalWrite(cs, HIGH);
    int a=n & B00001111;    
    if(i==2){ 
      int b=(n & B00110000)>>4; //24 hour mode
      if(b==B00000010)
        b=20;        
      else if(b==B00000001)
        b=10;
      TimeDate[i]=a+b;
    }
    else if(i==4){
      int b=(n & B00110000)>>4;
      TimeDate[i]=a+b*10;
    }
    else if(i==5){
      int b=(n & B00010000)>>4;
      TimeDate[i]=a+b*10;
    }
    else if(i==6){
      int b=(n & B11110000)>>4;
      TimeDate[i]=a+b*10;
    }
    else{ 
      int b=(n & B01110000)>>4;
      TimeDate[i]=a+b*10; 
      }
  }

//2020-12-31 23:55:00
  temp.concat("20");
  temp.concat(TimeDate[6]); // YEAR
  temp.concat("-") ;
  if (TimeDate[5] <10)
    temp.concat("0");
  temp.concat(TimeDate[5]); //MONTH 
  temp.concat("-") ;
  if (TimeDate[4] <10)
    temp.concat("0");
  temp.concat(TimeDate[4]); //DAY
  temp.concat(" ") ;
  if (TimeDate[2] <10)
    temp.concat("0");
  temp.concat(TimeDate[2]); // HH
  temp.concat(":") ;
  if (TimeDate[1] <10)
    temp.concat("0");
  temp.concat(TimeDate[1]); //MIN
  temp.concat(":") ;
  if (TimeDate[0] <10)
    temp.concat("0");
  temp.concat(TimeDate[0]);  // SEC

        *secp= TimeDate[0];
        *minp= TimeDate[1];
        *hrp= TimeDate[2];
  Serial.println(temp);      
  return(temp);
}


/*this part are reimplementation of the SD functions 
from MEGA to be written in the DUE */


/**

@brief    this function initializes the SD card

*/
int8_t initSD(void){
	String timeString;
        int sec1, min1, hr1, sec2, min2, hr2;
  
        File sdFile;
	unsigned int i,sentinel;
	SPI.setDataMode(SPI_MODE0); // 

	if (!SD.begin(6,chipSelect)) {
		Serial.println(" SDCARD initialization failed!");
		return -1;
	}
	Serial.println("SDCARD initialization Success");

	char name[10] = {};
        strcpy(name,MASTERNAME);
	strncat(name,".csv", 4);
	
	sdFile = SD.open(name,FILE_WRITE);
	if(!sdFile){
		Serial.println("Failed to create logger file");
		return -2;
	}
	
	Serial.println("created the name file");			
        //write the current date and stuff
  	
	sdFile.close();//close the file
	SD.end();
	
	return 0;

}

int8_t writeData(String data){
	File sdFile;
	char filename[100]= {};
	String timeString;
	int sec1, min1, hr1, sec2, min2, hr2;
	SPI.setDataMode(SPI_MODE0); // switch mode to SD
	SPI.setBitOrder(LSBFIRST); 
	Serial.println(TIMESTAMP);


	if (!SD.begin(6,chipSelect)) {
		Serial.println(" SDCARD: walang timestamp!");
		return -1;
	}

	delay(20);
	loggerFileName= {};
	for(int i=0; i<6 ; i++){
		loggerFileName[i]= TIMESTAMP[i];
	}

	strcpy(filename,loggerFileName);

	strcat(filename,".TXT");
	Serial.println(filename);

	sdFile = SD.open(filename,FILE_WRITE);
	if(!sdFile){
		Serial.println("Can't Write to file");
		return -1;
	}
	sdFile.print(MASTERNAME);
	sdFile.print(",");
	sdFile.print(data);
	sdFile.print(",");

	//get timestamp from what was given by the MEGA
	sdFile.print(TIMESTAMP);
	sdFile.println();
	sdFile.close();//close the file
	SD.end();
	SPI.setDataMode(SPI_MODE3); // switch mode to clock
	Serial.println("writing to SD"); 
}
void getValuesfromEEPROM(){
  
  Serial.println("-------------------------------------------------------------");
  Serial.println("Successfully assigned values from EEPROM to global variables. ");
  Serial.println("-------------------------------------------------------------");
  //lagyan ng lam yung mga global variables
  
  MASTERNAME[0]= (char)dueFlashStorage.read(3);
  MASTERNAME[1]= (char)dueFlashStorage.read(4);
  MASTERNAME[2]=(char)dueFlashStorage.read(5);
  MASTERNAME[3]=(char)dueFlashStorage.read(6);
  MASTERNAME[4]=(char)dueFlashStorage.read(7);
  MASTERNAME[5]= '\0';
  
  Serial.print("MASTERNAME : "); Serial.println(MASTERNAME);
  
  sensorVersion = (int)dueFlashStorage.read(16);
  Serial.print("sensorVersion: "); Serial.println(sensorVersion); 
  
  dataloggerVersion = (int)dueFlashStorage.read(15);
  Serial.print("dataloggerVersion: "); Serial.println(dataloggerVersion);

  ColumnCommand = (char)dueFlashStorage.read(1);
  Serial.print("ColumnCommand : "); Serial.println(ColumnCommand); 
  
  numOfNodes = (int)dueFlashStorage.read(0);
  Serial.print("numOfNodes : "); Serial.println(numOfNodes);

  
  for (int i=0; i< numOfNodes; i++){
      int tempcolid= (((int)dueFlashStorage.read(20 + (i*2)))*100) + ((int)dueFlashStorage.read(21 + (i*2)));
      GIDTable[i][1] = tempcolid;
      Serial.print("GIDTable["); Serial.print(i+1); Serial.print("]  =  "); Serial.println(GIDTable[i][1]);		   
  }

  REPEATING_FRAMES_LIMIT= (int)dueFlashStorage.read(11);
  Serial.print("REPEATING_FRAMES_LIMIT : "); Serial.println(TURN_ON_DELAY);
  
  NO_COLUMN_LIMIT= (int)dueFlashStorage.read(8);  
  Serial.print("NO_COLUMN_LIMIT : "); Serial.println(NO_COLUMN_LIMIT);
  
  TURN_ON_DELAY = (int)dueFlashStorage.read(9);
  Serial.print("TURN_ON_DELAY : "); Serial.println(TURN_ON_DELAY); 
   
  PRINT_MODE = (int)dueFlashStorage.read(13);
  Serial.print("PRINT_MODE : "); Serial.println(PRINT_MODE); 
  
  PIEZO = (int)dueFlashStorage.read(18);
  Serial.print("PIEZO: "); Serial.println(PIEZO);
  
 


//  numOfNodes= 11;
//  MASTERNAME[0]= 'K';
//  MASTERNAME[1]= 'E';
//  MASTERNAME[2]= 'N';
//  MASTERNAME[3]= 'T';
//  MASTERNAME[4]= 'A';
//  MASTERNAME[5]= '\0'; 
//  
//  NO_COLUMN_LIMIT= 2;  
//  
//  TURN_ON_DELAY = 10;
//  TURN_ON_DELAY= TURN_ON_DELAY * 100;
//  
//  enable_find_node_ids= 0;
//  
//  REPEATING_FRAMES_LIMIT= 2;
//  
//  ALLOWED_MISSING_NODES= 0;
//  
//  PRINT_MODE= 0;
//  
////  for (int i=0; i< numOfNodes; i++){
////      int tempcolid= (((int)dueFlashStorage.read(20 + (i*2)))*100) + ((int)dueFlashStorage.read(21 + (i*2)));
////      GIDTable[i][1] = tempcolid;		   
////  }
//  GIDTable[0][1] = 2267;
//  GIDTable[1][1] = 2764;
//  GIDTable[2][1] = 2898;
//  GIDTable[3][1] = 2949;
//  GIDTable[4][1] = 2918;
//  GIDTable[5][1] = 2912; 
//  GIDTable[6][1] = 3006;
//  GIDTable[7][1] = 3086;
//  GIDTable[8][1] = 3088;
//  GIDTable[9][1] = 3129;
//  GIDTable[10][1] = 2624;
//
//  COLUMN_COOL_OFF = 10 * 100;
//
//  sensorVersion = 2;
//
//  dataloggerVersion = 2;
 
}

