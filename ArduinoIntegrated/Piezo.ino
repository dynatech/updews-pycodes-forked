/*
  Piezometer Reading Code v08
    by Prado Arturo Bognot

  Last Updated: Nov 29, 2013 at 5:30 pm
  
  Expected VWP freq is from 2406 Hz to 2951 Hz	
*/

#include "PlainFFT_Prado.h"

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

/*
void setup(){
	Serial.begin(115200);
	Serial.println("Ready");

        //initialize
        ReadPiezometer ();
}
*/

double PiezoData() 
{
        //initialize
        ReadPiezometer ();    
  
        resultFrequency = 0;
        double freqSamples = 20.0;

        for (uint8_t i = 0; i < freqSamples; i++) {
                ReadPiezometer ();
	}

        resultFrequency = resultFrequency / freqSamples;
        Serial.print("Frequency: ");
	Serial.println(resultFrequency, 6);

	return resultFrequency;
}



void ReadPiezometer ()
{
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
	double y = FFT.MajorPeak(vReal, samples, samplingFrequency, minFrequency);
        double x = 0;
        x = y - 25.09;

        resultFrequency += x;

        //Clear the data arrays
	for (uint8_t i = 0; i < samples; i++) {
		vReal[i] = 0;
	        vImag[i] = 0;
        }
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
        
        /*
        tone(pinSweep, 2800);
        delay(20);
        noTone(pinSweep);
        */
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


