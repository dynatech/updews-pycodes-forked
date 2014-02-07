/*
  Piezometer Reading Code v08
    by Prado Arturo Bognot

  Last Updated: Nov 29, 2013 at 5:30 pm
  
  Expected VWP freq is from 2406 Hz to 2951 Hz	
*/

//Just added a comment for the SVN testing

#include "PlainFFT_Prado.h"

#define WITH_DELAY 0
#define WITH_ERROR_CORRECTION 1
#define ERROR_MARGIN 2

PlainFFT_Prado FFT = PlainFFT_Prado(); /* Create FFT object */
/* 
These values can be changed in order to evaluate the functions 
*/
const uint16_t samples = 128;
//double signalFrequency = 2951;
double samplingPeriod = 0;
double forPeriod = 0;
double samplingFrequency = 0;  //100 microseconds period
double resultFrequency = 0;
double minFrequency = 2300.0;

double minRead = 9999.0;
double maxRead = -1.0;
double minHistRead = 9999.0;
double maxHistRead = -1.0;
double centerHist;

int delay_T = 100;

int pinSweep = 12;
int pinSwitch = 10;

/* 
These are the input and output vectors 
Input vectors receive computed results from FFT
*/
double vReal[samples]; 
double vImag[samples];

#define SCL_INDEX 0x00
#define SCL_TIME 0x01
#define SCL_FREQUENCY 0x02

void setup(){
	Serial.begin(115200);
	Serial.println("Ready");

        //initialize
        ReadPiezometer ();
}

void loop() 
{ 
    unsigned long start_T = 0;
    unsigned long end_T = 0;

    // Calculate the delay
    start_T = micros();
        
    Serial.println("Starting piezo frequency reading...");
    detFreqRange();

    Serial.print("Piezo Frequency: ");
    Serial.println(filterHistogram (), 6);
   
    end_T = micros();
    Serial.print("Delay: ");
    Serial.println((end_T - start_T) / 1000000.0, 6);
    Serial.println(" ");
    
    Serial.print("Hist FL: ");
    Serial.print(getHistMin(), 6);
    Serial.print(", Hist FH: ");
    Serial.print(getHistMax(), 6);
    Serial.print(", Fluctuations (in Hz): ");
    Serial.println(getHistMax() - getHistMin(), 6);
    Serial.println("\n");   
}

// Determine the Frequency Range of Piezometer Reading
void detFreqRange () {
    double freqSamples = 20.0;
    resultFrequency = 0;

    for (uint8_t i = 0; i < freqSamples; i++) {        
        resultFrequency += ReadPiezometer ();    
	}

    resultFrequency = roundDouble(resultFrequency / freqSamples);
    
    if(resultFrequency >= minFrequency) {
        Serial.print("Frequency: ");
	Serial.println(resultFrequency, 6);
        //detMinMax(resultFrequency);
    }
    else {
        //check your piezo connection
        Serial.println("check your piezo connection");
    }
    
    if (WITH_DELAY) {
        delay(delay_T * 10); 
    }
    
    setCenter(resultFrequency);
}
/*
void detFreqRange () {
    int cnt = 5;
  
    resetMinMax();
    while (cnt-- > 0) {
        double freqSamples = 5.0;
        resultFrequency = 0;

        for (uint8_t i = 0; i < freqSamples; i++) {        
            resultFrequency += ReadPiezometer ();    
	}

        resultFrequency = roundDouble(resultFrequency / freqSamples);
        
        if(resultFrequency >= minFrequency) {
            //Serial.print("Frequency: ");
  	    //Serial.println(resultFrequency, 6);
            detMinMax(resultFrequency);
        }
        else {
            //check your piezo connection
            Serial.println("check your piezo connection");
        }
        
        if (WITH_DELAY) {
            delay(delay_T * 10); 
        }
    }
    Serial.print("FL: ");
    Serial.print(getMin(), 6);
    Serial.print(", FH: ");
    Serial.print(getMax(), 6);
    Serial.print(", Fluctuations (in Hz): ");
    Serial.println(getMax() - getMin(), 6);
}
*/

double filterHistogram () {
    int steps = 20;
    double delta = (double) ERROR_MARGIN / (double) steps;
    double histResult;
    int hist[steps];
    int median = 0;
    int histSamples = 10 * steps;
    double minVal = getCenter() - (ERROR_MARGIN / 2);
    
    Serial.println("Using Histogram Analysis...");
    
    for (int h = 0; h < steps; h++) {
        hist[h] = 0;
    }
    
    for (int i = 0; i < histSamples; i++) {
        resultFrequency = ReadPiezometer();
        
        for (int j = 0; j < steps; j++ ) {
            double lowerBound = minVal + (delta * j) - (delta / 2);
            double upperBound = minVal + (delta * j) + (delta / 2);
            
            if((resultFrequency >= lowerBound) && (resultFrequency < upperBound)) {
                hist[j] += 1;
            }
        }
    }
    
    Serial.println("Finding the median...");
    for (int k = 0; k < steps; k++) {
        if (hist[k] > median) {
            median = hist[k];
        }
    }    
    Serial.print("Median: ");
    Serial.print(median);
    Serial.print("\tHits: ");
    Serial.println(hist[median]);
    
    histResult = minVal + (delta * ((double)median));
    detHistMinMax(histResult);
    
    return histResult;
}
/*
double filterHistogram () {
    int steps = 20;
    double delta = (getMax() - getMin()) / (double) steps;
    double histResult;
    int hist[steps];
    int median = 0;
    int histSamples = 10 * steps;
    
    Serial.println("Using Histogram Analysis...");
    
    for (int h = 0; h < steps; h++) {
        hist[h] = 0;
    }
    
    for (int i = 0; i < histSamples; i++) {
        resultFrequency = ReadPiezometer();
        
        for (int j = 0; j < steps; j++ ) {
            double lowerBound = getMin() + (delta * j);
            double upperBound = getMin() + (delta * (j + 1));
            
            if((resultFrequency >= lowerBound) && (resultFrequency < upperBound)) {
                hist[j] += 1;
            }
        }
    }
    
    Serial.println("Finding the median...");
    for (int k = 0; k < steps; k++) {
        if (hist[k] > median) {
            median = hist[k];
        }
    }    
    Serial.print("Median: ");
    Serial.print(median);
    Serial.print("\tHits: ");
    Serial.println(hist[median]);
    
    histResult = getMin() + (delta * ((double)median + 0.5));
    detHistMinMax(histResult);
    
    return histResult;
}
*/

void setCenter (double value) {
    double dVal = value * 10.0;
    int iVal = (int) dVal;
    
    centerHist = (double) iVal / 10.0;
    
    Serial.print("Center: ");
    Serial.println(centerHist);
}

double getCenter () {
    return centerHist;
}

void detMinMax (double readValue) {
    if (readValue < minRead) {
        minRead = readValue;
    }
    if (readValue > maxRead) {
        maxRead = readValue;
    }
}

void resetMinMax () {
    minRead = 9999.0;
    maxRead = -1.0;
}

double getMax () {
    return maxRead;
}

double getMin () {
    return minRead;
}

void detHistMinMax (double readValue) {
    if (readValue < minHistRead) {
        minHistRead = readValue;
    }
    if (readValue > maxHistRead) {
        maxHistRead = readValue;
    }
}

double getHistMax () {
    return maxHistRead;
}

double getHistMin () {
    return minHistRead;
}

double ReadPiezometer ()
{
    double y = 0;
    double x = 0;
  
    do {
	/* Build raw data */
        samplingPeriod = 0;
        forPeriod = 0;
        unsigned long start_T = 0;
        unsigned long end_T = 0;

        // Calculate the delay induced by the "For Loop"
        start_T = micros();
	for (uint8_t i = 0; i < samples; i++) {
                //vReal[i] = analogRead(A0);
	}
        end_T = micros();
        forPeriod = end_T - start_T; 

        // Do our frequency sweep
        FrequencySweep();

        start_T = micros();
	for (uint8_t i = 0; i < samples; i++) {
                vReal[i] = 10.0 * analogRead(A0);
	}
        end_T = micros();
        samplingPeriod = (end_T - start_T) - forPeriod; 

        samplingPeriod = samplingPeriod / 128.0;   
        
        samplingPeriod = samplingPeriod / 1000000.0; 
        
        samplingFrequency = 1.0 / samplingPeriod;

	FFT.Windowing(vReal, samples, FFT_WIN_TYP_HAMMING, FFT_FORWARD);	/* Weigh data */
	FFT.Compute(vReal, vImag, samples, FFT_FORWARD); /* Compute FFT */
	FFT.ComplexToMagnitude(vReal, vImag, samples); /* Compute magnitudes */
	//PrintVector(vReal, (samples >> 1), SCL_FREQUENCY);	
	y = FFT.MajorPeak(vReal, samples, samplingFrequency, minFrequency);
        y = roundDouble(y);
        x = y - 25.1;

        //Clear the data arrays
	for (uint8_t i = 0; i < samples; i++) {
		vReal[i] = 0;
	        vImag[i] = 0;
        }
    } while (y < minFrequency);

    if (WITH_ERROR_CORRECTION) {
        return x;
    }
    else {
        return y;
    }        
}

double roundDouble (double value) {
    int rem;
  
    rem = ((int)(value * 10) % 10);
    return ((int)(value) + ((double)rem / 10.0));
}

void FrequencySweep ()
{        
        digitalWrite(pinSwitch, LOW);
 
        for(int i = 2300; i < 3100; i++){
          tone(pinSweep, i, 10);
        }
        for(int i = 3100; i > 2300; i--){
          tone(pinSweep, i, 10);
        }
        noTone(pinSweep);

        //delay(25);
        digitalWrite(pinSwitch, HIGH);
        
        //have a few milli seconds of delay
        delay(20);
}

void PrintVector(double *vData, uint8_t bufferSize, uint8_t scaleType) 
{	
	for (uint16_t i = 0; i < bufferSize; i++) {
		double abscissa;
		/* Print abscissa value */
		switch (scaleType) {
		case SCL_INDEX:
			abscissa = (i * 1.0);
			break;
		case SCL_TIME:
			abscissa = ((i * 1.0) / samplingFrequency);
			break;
		case SCL_FREQUENCY:
			abscissa = ((i * 1.0 * samplingFrequency) / samples);
			break;
		}
		Serial.print(abscissa, 6);
		Serial.print(" ");
		Serial.print(vData[i], 4);
		Serial.println();
	}
	Serial.println();
}


