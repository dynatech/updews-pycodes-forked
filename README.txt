README.txt
Created by: Supervising SRS - Prado Bognot

/*****************************************************************************/

QUICK SETUP:

For a quick setup of requirements, run the shell script 'setup.sh' using the 
command below:

sh setup.sh

- setup.sh will install the following:
a. Required anaconda libraries
b. Required linux packages
c. Required pip packages

/*****************************************************************************/


ADDITIONAL ESSENTIAL LIBRARY:

a. Anaconda library
- If your code uses an essential anaconda library that isn't part of the 
"requirements_conda.txt" file then, do the following:

	i. Go to your "updews-pycodes" directory
	ii. Export your list of anaconda libraries

		conda list --export > requirements_conda.txt

	iii. Verify if the new anaconda library is present in the file
	iv. If verified that library name is part of "requirements_conda.txt" then,
		don't forget to git commit and push your changes to the file.