// This sketch demonstrates the use of the Gobetwino commandtypes LGFIL and CPFIL

int serInLen = 25;
char serInString[25];
int logValue1=0;
int logValue2=0;
int logValue3=0;
int result;

void setupOrig() 
{ 
  // Setup serial comm. Initialize random function.
  Serial.begin(9600); 
  randomSeed(analogRead(0));
  delay(5000); 
  // Use the CPTEST copy file command to make a copy of a new empty logfile
  Serial.println("#S|CPTEST|[]#");
  readSerialString(serInString,1000);
  // There ought to be a check here for a non 0 return value indicating an error and some error handeling
} 
 
void loopOrig() 
{ 
   //Create some random values to log to a file on the PC.  This could be sensor readings or whatever
   //but for this example it's just 3 random values
   logValue1= random(0,1000);
   logValue2= random(0,1000);
   logValue3= random(0,1000);
   //logData(logValue1,logValue2,logValue3);
   delay(500); 
}

// Send the LOGTEST command to Gobetwino the 3 random values are seperated by semicolons
 
void logDataDouble( double value1 ) 
{
   char buffer[5];
   int whole = (int)(value1 * 10.0);
   double decimalDouble = (value1 - whole) * 10.0;
   int decimalInt = (int)decimalDouble * 1;
  
   Serial.print("#S|LOGTEST|[");
   Serial.print(itoa((whole), buffer, 10));
   //Serial.println(".");
   //Serial.print(itoa((decimalInt), buffer, 10));
   Serial.println("]#");
   readSerialString(serInString,1000);
   // There ought to be a check here for a non 0 return value indicating an error and some error handeling
}   
 
void logData( int value1 ) 
{
   char buffer[5];
  
   Serial.print("#S|LOGTEST|[");
   Serial.print(itoa((value1), buffer, 10));
   Serial.println("]#");
   readSerialString(serInString,1000);
   // There ought to be a check here for a non 0 return value indicating an error and some error handeling
}   
 
void logData2( int value1, int value2 ) 
{
   char buffer[5];
  
   Serial.print("#S|LOGTEST|[");
   Serial.print(itoa((value1), buffer, 10));
   Serial.print(";");
   Serial.print(itoa((value2), buffer, 10));
   Serial.println("]#");
   readSerialString(serInString,1000);
   // There ought to be a check here for a non 0 return value indicating an error and some error handeling
}  
 
void logData3( int value1, int value2, int value3) 
{
   char buffer[5];
  
   Serial.print("#S|LOGTEST|[");
   Serial.print(itoa((value1), buffer, 10));
   Serial.print(";");
   Serial.print(itoa((value2), buffer, 10));
   Serial.print(";");
   Serial.print(itoa((value3), buffer, 10));
   Serial.println("]#");
   readSerialString(serInString,100);
   // There ought to be a check here for a non 0 return value indicating an error and some error handeling
} 

//read a string from the serial and store it in an array
//you must supply the array variable - return if timeOut ms passes before the sting is read
void readSerialString (char *strArray,long timeOut) 
{
   long startTime=millis();
   int i;

   while (!Serial.available()) {
      if (millis()-startTime >= timeOut) {
         return;
      }
   }
   while (Serial.available() && i < serInLen) {
      strArray[i] = Serial.read();
      i++;
   }
}
