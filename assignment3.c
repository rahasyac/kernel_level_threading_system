// Name: Fnu Rahasya Chandan
// UTA Id: 100954962

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>

//buffer size
#define BUFFER_SIZE 50
//buffer big size
#define BUFFER_BIG_SIZE 300
//input file name
#define INPUT_FILENAME "all_month.csv"
//each line contains 22 columns
#define RECORD_SIZE 22

/*Earth quake data*/
typedef struct EarthQuake{
	char time[BUFFER_SIZE];
	double latitude;
	double longitude;
	double depth;
	double mag;
	char magType[BUFFER_SIZE];
	int nst;
	double gap;
	double dmin;
	double rms;
	char net[BUFFER_SIZE];
	char id[BUFFER_SIZE];
	char updated[BUFFER_SIZE];
	char place[BUFFER_SIZE];
	char type[BUFFER_SIZE];
	double horizontalError;	
	double depthError;
	double magError;
	int magNst;
	char status[BUFFER_SIZE];
	char locationSource[BUFFER_SIZE];
	char magSource[BUFFER_SIZE];	
	
} EarthQuake;

typedef struct ThreadArgs
{
    EarthQuake* dataArray; //array of EarthQuake* pointers

	int from;
	int to;

	//number of threads the thread can create
	int numThreads;

	pthread_mutex_t* accessMutex;

} ThreadArgs;

/*
one task, run by one thread
*/
void * do_thread_job(void * arg);

/*
read number of lines
*/
int readNumLines();

/*
load data
*/
void loadData(EarthQuake* dataArray, int size);

/*bubble sort on partial array*/
void bubleSort(EarthQuake* dataArray, int from, int to);

/*parallel sort*/
void parallelSort(EarthQuake* dataArray, int from, int to, int numThreads, pthread_mutex_t* accessMutex);

int main(){

	//number of threads
	int numThreads = 10;

	int i;

	pid_t aProcessID;

	int status;

	struct timespec begintime; 
	struct timespec endtime; 

	EarthQuake* dataArray; //array of pointers (each element is a pointer to the EarthQuake structure)

	pthread_mutex_t accessMutex;

	int numLines = readNumLines();

	pthread_mutex_init(&accessMutex, NULL);	

	dataArray = (EarthQuake*)mmap(NULL, numLines * sizeof(EarthQuake), PROT_READ | PROT_WRITE, 
               MAP_SHARED | MAP_ANONYMOUS, -1, 0);

	if (dataArray == NULL)
	{
		printf("Error while allocate the memory.\n");
		exit(EXIT_FAILURE);
	}

	printf("Please enter number of threads: 2, 4, 10,...: ");
	scanf("%d", &numThreads);

	//load data
	loadData(dataArray, numLines);

	clock_gettime(CLOCK_REALTIME, &begintime);

	aProcessID = fork(); 

	if (aProcessID >= 0) // fork was successful 
	{
		if (aProcessID == 0) // child process 
		{
			parallelSort(dataArray, 0, numLines - 1, numThreads, &accessMutex);

			exit(0);
		}

	}else // fork failed 
	{ 
		printf("\nFork failed!\n"); 
		exit(0);
	} 


	wait(&status);


	clock_gettime(CLOCK_REALTIME, &endtime);
	if (endtime.tv_nsec < begintime.tv_nsec) {
        endtime.tv_nsec += 1000000000;
        endtime.tv_sec--;
    }

	 printf("Elapsed time: %ld.%09ld\n", (long)(endtime.tv_sec - begintime.tv_sec),
        endtime.tv_nsec - begintime.tv_nsec);

	pthread_mutex_destroy(&accessMutex);

	//free memory
    munmap(dataArray, numLines * sizeof(EarthQuake));

	return 0;
}

int readNumLines(){

	FILE *fp; //File structure pointer
	char buffer[BUFFER_BIG_SIZE];
	int lines = -1;

	//open file for reading
	fp = fopen(INPUT_FILENAME, "r"); 

	if (fp == NULL)
	{
		printf("Error while opening the file.\n");
		exit(EXIT_FAILURE);
	}

	while (fgets(buffer, BUFFER_BIG_SIZE, fp) != NULL){

		lines++;
	}

	fclose(fp);

	return lines;
}


void loadData(EarthQuake* dataArray, int size){

	FILE *fp; //File structure pointer
	char buffer[BUFFER_BIG_SIZE];
	int lines = 0;
	int i;
	char tokens[RECORD_SIZE][BUFFER_SIZE];

	//open file for reading
	fp = fopen(INPUT_FILENAME, "r"); 

	//ignore header line
	fgets(buffer, BUFFER_BIG_SIZE, fp);

	//read line by line until end of file
	for (i = 0; i < size; i++)
	{
		fgets(buffer, BUFFER_BIG_SIZE, fp);
				
		sscanf(buffer, "%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],\"%[^\"]\",%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%[^\n]\n", 
			tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], tokens[6], 
			tokens[7], tokens[8], tokens[9], tokens[10], tokens[11], tokens[12], tokens[13], 
			tokens[14], tokens[15], tokens[16], tokens[17], tokens[18], tokens[19], tokens[20], tokens[21]);
										
		strcpy(dataArray[lines].time, tokens[0]);
				
		if (strcmp(tokens[1], "") == 0)
		{
			dataArray[lines].latitude = 0;
		}else{
			sscanf(tokens[1], "%lf", &dataArray[lines].latitude);
		}
				
		if (strcmp(tokens[2], "") == 0)
		{
			dataArray[lines].longitude = 0;
		}else{
			sscanf(tokens[2], "%lf", &dataArray[lines].longitude);
		}
				
		if (strcmp(tokens[3], "") == 0)
		{
			dataArray[lines].depth = 0;
		}else{
			sscanf(tokens[3], "%lf", &dataArray[lines].depth);
		}
				
		if (strcmp(tokens[4], "") == 0)
		{
			dataArray[lines].mag = 0;
		}else{
			sscanf(tokens[4], "%lf", &dataArray[lines].mag);
		}
							
		strcpy(dataArray[lines].magType, tokens[5]);				
			
		if (strcmp(tokens[6], "") == 0)
		{
			dataArray[lines].nst = 0;
		}else{
			sscanf(tokens[6], "%d", &dataArray[lines].nst);
		}
				
		if (strcmp(tokens[7], "") == 0)
		{//empty data
			dataArray[lines].gap = 0;
		}else{
			sscanf(tokens[7], "%lf", &dataArray[lines].gap);
		}
				
		if (strcmp(tokens[8], "") == 0)
		{
			dataArray[lines].dmin = 0;
		}else{
			sscanf(tokens[8], "%lf", &dataArray[lines].dmin);
		}
				
		if (strcmp(tokens[9], "") == 0)
		{
			dataArray[lines].rms = 0;
		}else{
			sscanf(tokens[9], "%lf", &dataArray[lines].rms);
		}
								
		strcpy(dataArray[lines].net, tokens[10]);
							
		strcpy(dataArray[lines].id, tokens[11]);
								
		strcpy(dataArray[lines].updated, tokens[12]);
							
		strcpy(dataArray[lines].place, tokens[13]);
							
		strcpy(dataArray[lines].type, tokens[14]);
							
		if (strcmp(tokens[15], "") == 0)
		{
			dataArray[lines].horizontalError = 0;
		}else{
			sscanf(tokens[15], "%lf", &dataArray[lines].horizontalError);
		}
								
		if (strcmp(tokens[16], "") == 0)
		{
			dataArray[lines].depthError = 0;
		}else{
			sscanf(tokens[16], "%lf", &dataArray[lines].depthError);
		}
								
		if (strcmp(tokens[17], "") == 0)
		{
			dataArray[lines].magError = 0;
		}else{
			sscanf(tokens[17], "%lf", &dataArray[lines].magError);
		}
								
		if (strcmp(tokens[18], "") == 0)
		{
			dataArray[lines].magNst = 0;
		}else{
			sscanf(tokens[18], "%d", &dataArray[lines].magNst);
		}
								
		strcpy(dataArray[lines].status, tokens[19]);
					
		strcpy(dataArray[lines].locationSource, tokens[20]);	
							
		strcpy(dataArray[lines].magSource, tokens[21]);				

		lines = lines + 1;
	}

	//close file
	fclose(fp);
}

/*bubble sort on partial array*/
void bubleSort(EarthQuake* dataArray, int from, int to){


	int i, j;

	for (i = from; i < to; i++)
	{
		for (j = i + 1; j <= to; j++)
		{
			if (dataArray[i].latitude > dataArray[j].latitude)
			{
				//swap
				EarthQuake temp = dataArray[i];
				dataArray[i] = dataArray[j];
				dataArray[j] = temp;
			}
		}
	}
}

void parallelSort(EarthQuake* dataArray, int from, int to, int numThreads, pthread_mutex_t* accessMutex){
	
	int i;
	int status;
	int left, right;

	pthread_t firstThreadID;
	pthread_t secondThreadID;

	ThreadArgs firstThreadArg;
	ThreadArgs secondThreadArg;

	int firstRemainingThreads;
	int secondRemainingThreads;

	EarthQuake* tempDataArray;

	int middle;

	int subArraylength;

	int subArrayMiddle;

	if (from >= to)
	{
		return;
	}

	if (numThreads == 0)
	{
		bubleSort(dataArray, from, to);

	}else if (numThreads == 1){
        
		subArraylength = (to - from + 1);

		middle = (from + to) / 2;	

		subArrayMiddle = middle - from;

		tempDataArray = (EarthQuake*)malloc(subArraylength * sizeof(EarthQuake));

		firstThreadArg.accessMutex = accessMutex;
		firstThreadArg.dataArray = dataArray;
		firstThreadArg.from = middle + 1;
		firstThreadArg.to = to;
		firstThreadArg.numThreads = 0;

		if (pthread_create (&firstThreadID, NULL, do_thread_job, (void *) &firstThreadArg) !=0) {
			printf("ERROR creating thread\n");
			return;
		}

		pthread_mutex_lock(accessMutex);

		for (i = 0; i <= subArrayMiddle; i++)
		{
			tempDataArray[i] = dataArray[from + i];
		}

		pthread_mutex_unlock(accessMutex);

		bubleSort(tempDataArray, 0, subArrayMiddle);

		pthread_join(firstThreadID, NULL);

		pthread_mutex_lock(accessMutex);

		//copy to temp
		for (i = subArrayMiddle + 1; i <= subArraylength - 1; i++)
		{
			tempDataArray[i] = dataArray[from + i];
		}

		pthread_mutex_unlock(accessMutex);

		//merge		
		i = from;
		left = 0;
		right = subArrayMiddle + 1;

		pthread_mutex_lock(accessMutex);

		while (left <= subArrayMiddle && right <= subArraylength - 1)
		{			
			if (tempDataArray[left].latitude <= tempDataArray[right].latitude)
			{					
				dataArray[i++] = tempDataArray[left++];
			}else{
				dataArray[i++] = tempDataArray[right++];
			}
			
		}
		while (left <= subArrayMiddle)
		{
			dataArray[i++] = tempDataArray[left++];
		}

		while (right <= subArraylength - 1)
		{
			dataArray[i++] = tempDataArray[right++];
		}

		pthread_mutex_unlock(accessMutex);

		//free resource
		free(tempDataArray);

	}else{


		subArraylength = (to - from + 1);

		//middle of data array
		middle = (from + to) / 2;	

		//middle of sub array
		subArrayMiddle = middle - from;

		firstRemainingThreads = (numThreads - 2 ) / 2;
		secondRemainingThreads = firstRemainingThreads;

		//add one thread to first if odd
		if (((numThreads - 2 ) / 2) % 2 == 1)
		{
			firstRemainingThreads = firstRemainingThreads + 1;
		}		

		middle = (from + to) / 2;

		//create first argument
		firstThreadArg.accessMutex = accessMutex;
		firstThreadArg.dataArray = dataArray;
		firstThreadArg.from = from;
		firstThreadArg.to = middle;
		firstThreadArg.numThreads = firstRemainingThreads;

		//create second argument
		secondThreadArg.accessMutex = accessMutex;
		secondThreadArg.dataArray = dataArray;
		secondThreadArg.from = middle + 1;
		secondThreadArg.to = to;
		secondThreadArg.numThreads = secondRemainingThreads;

		if (pthread_create (&firstThreadID, NULL, do_thread_job, (void *) &firstThreadArg) !=0) {
			printf("ERROR creating thread\n");
			return;
		}

		if (pthread_create (&secondThreadID, NULL, do_thread_job, (void *) &secondThreadArg) !=0) {
			printf("ERROR creating thread\n");
			return;
		}

		pthread_join(firstThreadID, NULL);
		pthread_join(secondThreadID, NULL);

		tempDataArray = (EarthQuake*)malloc(subArraylength * sizeof(EarthQuake));
	
		pthread_mutex_lock(accessMutex);

		for (i = 0; i < subArraylength; i++)
		{
			tempDataArray[i] = dataArray[from + i];
		}

		pthread_mutex_unlock(accessMutex);

        i = from;
		left = 0;
		right = subArrayMiddle + 1;

		pthread_mutex_lock(accessMutex);

		while (left <= subArrayMiddle && right <= subArraylength - 1)
		{			
			if (tempDataArray[left].latitude <= tempDataArray[right].latitude)
			{					
				dataArray[i++] = tempDataArray[left++];
			}else{
				dataArray[i++] = tempDataArray[right++];
			}
			
		}
		while (left <= subArrayMiddle)
		{
			dataArray[i++] = tempDataArray[left++];
		}

		while (right <= subArraylength - 1)
		{
			dataArray[i++] = tempDataArray[right++];
		}

		pthread_mutex_unlock(accessMutex);

		//free resource
		free(tempDataArray);
	}	
}


void * do_thread_job(void * arg){

	ThreadArgs* args = (ThreadArgs*)arg;

	parallelSort(args->dataArray, args->from, args->to, args->numThreads, args->accessMutex);

	pthread_exit(NULL);
}
