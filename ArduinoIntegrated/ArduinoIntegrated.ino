#include <avr/power.h>
#include <avr/sleep.h>
#include <avr/eeprom.h>
//MMC
#include <SD.h>
#define BUILDNUMBER "072013-B"
// 071813 - changed timeout of sending data in GSM.pde
// 071813-B - added global time check after gsm operations
#define MASTERNAME "PIEZ"
//#define SERVERNUM "09094427156"	// SMART
//#define MOBILENUM "09202123558"	// SMART
//#define SERVERNUM "09227321596"	// SUN
//#define MOBILENUM "09236382723"	// SUN
#define SERVERNUM "09162408795"	// GLOBE
//#define MOBILENUM "09156658490"	// GLOBE -> expired
//#define SERVERNUM "09163677476"         // GLOBE Prado
#define MOBILESERVER "09274043858"      // GLOBE Prepaid Mobile Server
#define MOBILENUM "09176763541"	        // GLOBE Postpaid

#define CMDKEY "*#CMD#*"

#define CLEAR 1  // clear buffer after query
#define SAVE  0  // save buffer after query
#define OFF 1  // stat pin is HIGH (1) so it needs to be turn off
#define ON  0  // stat pin is LOW (0) so it needs to be turn on
#define SENDTIMEINTERVAL 10  // report interval in minutes
#define RELAYPIN 26
#define STATPIN 6
#define TURN_COLUMN_ON digitalWrite(RELAYPIN, HIGH);
#define TURN_COLUMN_OFF digitalWrite(RELAYPIN, LOW);
#define SAVEMSG false
#define DELMSG true

#define MIN_FREQ 2300.0

//File myFile;
char sleep = 0;

/************************************************************************************/

void setup(){
	unsigned long timestart = 0;
	unsigned long timenow = 0;
	boolean exit = false;
	char a = 0;
	char b = 0;
	// initialize serial comms

	sleep = 0;
	Serial.begin(57600);     // arduino debug
	Serial1.begin(9600);    // master column
	Serial2.begin(9600);
	pinMode(53, OUTPUT);
	pinMode(RELAYPIN, OUTPUT);
	WDT_off();

	Serial.print("\nSENSLOPE ");
	Serial.print(MASTERNAME);
	Serial.println(" MASTER BOX");
	Serial.print("Build no:  ");
	Serial.println(BUILDNUMBER);
	
	/* Enter GSM control state */
	///*
	timestart = millis();
	timenow = millis();
	Serial.println("Press anything to Enter GSM Control mode");
	while( timenow - timestart < 3000 ){
		timenow = millis();
		
		///*
		if (Serial.available()){
                        Serial.println("Pressed something... PANB");
			PowerGSM(ON);
			delay(1000);
			Serial2.flush();
			while(1){
				if(Serial.available()){
					a = Serial.read();
					if ( a == '^' ){
					    exit = true;
			                    break;
					}
					else 
                                            Serial2.print(a);
				}
				if(Serial2.available()){
					//b = ;
					Serial.print((char)Serial2.read());
				}
			}
		}
		
		if (exit) 
                    break;
	}
	
	if (timenow - timestart > 3000 ) 
            Serial.println("GSM Control not entered.");
	else 
            Serial.println("Exiting GSM Control");
	//*/
}

short sampFlg = 1;
short resetFlg = 0;
short resFlg = 0;
volatile long globalTime = 0;
char *columnData = NULL;

char *msgToSend = NULL;
char *FinalMsg = NULL;
char Timestamps[17];
char filenameDate[12];

unsigned int k = 0;
double piezoFreq;

void loop(){
        piezoFreq = 0;
        char piezoStr[16];
  
	char *buffer = NULL;
	
	char *temp2 = NULL;
	//char *messages = NULL;
	unsigned char msgno = 0;
	//char *ptr = NULL;
	char tokens = 0;
	char *stat = NULL;
	char *number = NULL;
	char *temp = NULL;
	//char *msg = NULL;
	
	/***********************************************************/
	/*	Memory allocations
	/***********************************************************/
	
	temp2 = (char *) calloc(15, sizeof(char *));
	temp = (char *) calloc(25, sizeof(char *));
	//messages = (char *) calloc(300, sizeof(char *));
	number = (char *) calloc(10, sizeof(char *));
	//msg = (char *) calloc(160, sizeof(char *));
	//ptr = (char *) calloc(100, sizeof(char *));
	
	//columnData = (char *) calloc(750, sizeof(char *));

	
	msgToSend = (char *) calloc(160, sizeof(char *));   
	buffer = (char *) calloc(100, sizeof(char *));
		
	/***********************************************************/
	/*	Sampling data from column
	/***********************************************************/
	
	float columnLen = 0.0;
	float loopnum = 0.0;
	short retry = 0;
	
	/***********************************************************/
	/*	Initializing GSM module and global time variables
	/***********************************************************/
//Prado +++
        TURN_COLUMN_ON;
	InitGSM();

        //get piezometer data
        //PiezoData2(&piezoFreq);
        do {
          piezoFreq = PiezoData();
        } while (piezoFreq < MIN_FREQ);
        
        Serial.print("\nRead VWP Frequency: ");
        Serial.println(piezoFreq, 6);
        dtostrf(piezoFreq, 5, 6, piezoStr);
	
	globalTime = GetTimestampTag(Timestamps);
	Serial.print("\nGlobalTime: ");
	Serial.println(globalTime, DEC);
	time_conf();
	
        char Hello[] = " in Hz\n";
	sprintf(msgToSend, "%s*", MASTERNAME);
        strncat(msgToSend, piezoStr, strlen(piezoStr));
        strncat(msgToSend, Hello, strlen(Hello));
	strncat(msgToSend, "*", 1);
	strncat(msgToSend, Timestamps, strlen(Timestamps));

	SendMsg(SERVERNUM, msgToSend, buffer);
	TURN_COLUMN_OFF;
//Prado ---

	free(msgToSend);	fix28135_malloc_bug();
	free(FinalMsg);		fix28135_malloc_bug();
	free(buffer);		fix28135_malloc_bug();
	//free(columnData);	fix28135_malloc_bug();
	free(number);	        fix28135_malloc_bug();
	free(temp2);	        fix28135_malloc_bug();
	//free(messages);	fix28135_malloc_bug();
	//free(ptr);		fix28135_malloc_bug();
	free(temp);		fix28135_malloc_bug();
	
	
	// in case of long message sending times
	// get globaltime again
	globalTime = GetTimestampTag(Timestamps);
	Serial.print("\nGlobalTime: ");
	Serial.println(globalTime, DEC);
	time_conf();
    
    
	/***********************************************************/
	/*	Powering down whole system
	/***********************************************************/
        PowerGSM(OFF);
        Serial.print("Entering sleep mode...");
	
	sleepNow();	
	
	Serial.println(" done");
	delay(3000);
}

void time_conf(){
  TIMSK5 &= ~(1<<TOIE5);  
  TCCR5A &= ~((1<<WGM51) | (1<<WGM50));  
  TCCR5B &= ~(1<<WGM22);  
  ASSR &= ~(1<<AS2);   
  TIMSK5 &= ~(1<<OCIE5A);    
  TCCR5B |= (1<<CS52);
  TCCR5B &= ~(1<<CS51);
  TCCR5B |= (1<<CS50);     
  TCNT5 = 49911;
  TIMSK5 |= (1<<TOIE5);  
} 

/*
  globalTime -> variable to represent time in seconds from nearest hour
  i.e.
  
  GSM time: 11:10:26 >> globalTime = 10*60 + 26
  GSM time: 09:25:15 >> globalTime = 25*60 + 15
  
  Sampling will start when globalTime % SENDTIMEINTERVAL = 0
  In other words, when globalTime is a factor of SENDTIMEINTERVAL
  
*/
ISR(TIMER5_OVF_vect)
{ 
	//Serial.print(TCNT5, DEC);
	TCNT5 = 49911;
	
	globalTime++;
  
	if (globalTime % (SENDTIMEINTERVAL*60) == 0){
		sleep_disable();
		power_all_enable();
		asm volatile ("jmp 0x0000");
	}
}

void sleepNow(){
  set_sleep_mode(SLEEP_MODE_IDLE);   // sleep mode is set here

  sleep_enable();          // enables the sleep bit in the mcucr register
                           // so sleep is possible. just a safety pin 
  
  power_adc_disable();
  power_spi_disable();
  power_timer0_disable();
  power_timer1_disable();
  power_timer2_disable();
  power_timer3_disable();
  power_timer4_disable();
  power_twi_disable();
  
  sleep_mode();        
}

void WDT_off(void){
  asm("cli");
  asm("wdr");
  
  MCUSR &= ~(1<<WDRF);
  WDTCSR |= (1<<WDCE) | (1<<WDE);
  WDTCSR = 0x00;
  asm("sei");
}

void WDT_Prescaler_Change(void){
  asm("cli");
  asm("wdr");
  
  WDTCSR |= (1<<WDCE) | (1<<WDE);
  WDTCSR = (1<<WDE) | (1 <<WDP3) | (1<<WDP0);
  asm("sei");
}

void clearString(char *strArray) {
  int j;
  for (j = 100; j > 0; j--)
    strArray[j] = 0x00;
}

