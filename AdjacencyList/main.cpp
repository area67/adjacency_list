#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <vector>
#include <stdlib.h>
#include <set>
#include <thread>
#include <mutex>
#include <boost/random.hpp>
#include <sched.h>
#include <pthread.h>
#include <iostream>
#include <iomanip>
#include "AdjacencyList.h"
#include "ThreadData.h"

//Default Values
int test_size = 10000;
int transaction_size = 4;
int num_thread = 4;
int key_range = 5000;
double insert_vertex_ratio = .4;
double delete_vertex_ratio = .4;
double insert_edge_ratio = .1;
double delete_edge_ratio = .1;
double find_ratio = 0;

double insert_percent = (insert_vertex_ratio * 100);
double delete_percent = insert_percent + (delete_vertex_ratio * 100);
double insert_edge_percent = delete_percent + (insert_edge_ratio * 100);
double delete_edge_percent = insert_edge_percent + (delete_edge_ratio * 100);
double find_percent = delete_edge_percent + (find_ratio * 100);

AdjacencyList *list;
ThreadData *t_data;

void *listTest(void *threadid)
{
	list->Init();

    boost::mt19937 randomGen;
    randomGen.seed(time(0));
    boost::uniform_int<uint32_t> key_dist(1, key_range);
    boost::uniform_int<uint32_t> operation_dist(0, 100);

    for(int i = 0; i < test_size; i++)
    {
    	Desc *desc = list->AllocateDesc(transaction_size);

        for(int t = 0; t < transaction_size; t++)
        {
            int op = operation_dist(randomGen);

	        if (op <= insert_percent)
	        {
                //std::cout << "Inserting\n";
	            desc->ops[t].type = INSERT;
	            desc->ops[t].key = key_dist(randomGen);
	        }
	        else if (op <= delete_percent)
	        {
                //std::cout << "Deleting\n";
	            desc->ops[t].type = DELETE;
	            desc->ops[t].key = key_dist(randomGen);
	        }
	        else if (op <= insert_edge_percent)
	        {
                //std::cout << "Inserting Edge\n";
	        	desc->ops[t].type = INSERT_EDGE;
	            desc->ops[t].key = key_dist(randomGen);
	            desc->ops[t].edge_key = key_dist(randomGen);
	        }
	        else if (op <= delete_edge_percent)
	        {
                //std::cout << "Deleting Edge\n";
	        	desc->ops[t].type = DELETE_EDGE;
	            desc->ops[t].key = key_dist(randomGen);
	            desc->ops[t].edge_key = key_dist(randomGen);
	        }
	        else if (op <= find_percent)
	        {
	        	desc->ops[t].type = FIND;
	            desc->ops[t].key = key_dist(randomGen);
	        }
        }

        if (list->ExecuteOps(desc))
        {
            t_data[(intptr_t)threadid].g_commits++;
        }
        else
        {
            t_data[(intptr_t)threadid].g_aborts++;
        }
    }
}

void prePopulateList()
{
    list->Init();
    
    for (int i = 1; i < key_range; i++)
    {
        Desc *desc = list->AllocateDesc(transaction_size);

        desc->ops[0].type = INSERT;
        desc->ops[0].key = i;

        if (!list->ExecuteOps(desc))
        {
            std::cout << "Error\n";
        }
    }
}

int main(int argc, const char *argv[])
{
	struct timespec start, finish;
    double elapsed;

    if (argc < 9)
    {
        printf("Proper format: %s <#TestSize> <#TransactionSize> <#Threads> <#KeyRange> <InsertVertex Ratio> <DeleteVertex Ratio> <InsertEdge Ratio> <DeleteEdge Ratio> <Find Ratio>\n", argv[0]);
        printf("All operation ratios should sum to 1.0\n");
        std::exit(EXIT_FAILURE);
    }

    test_size = atoi(argv[1]);
    transaction_size = atoi(argv[2]);
    num_thread = atoi(argv[3]);
    key_range = atoi(argv[4]);
    insert_vertex_ratio = std::stod(argv[5]);
    delete_vertex_ratio = std::stod(argv[6]);
    insert_edge_ratio = std::stod(argv[7]);
    delete_edge_ratio = std::stod(argv[8]);
    find_ratio = std::stod(argv[9]);

    if (insert_vertex_ratio + delete_vertex_ratio + insert_edge_ratio + delete_edge_ratio + find_ratio != 1.0)
    {
        printf("Error, operation ratios do not sum to 1.0\n");
        std::exit(EXIT_FAILURE);
    }

    list = new AdjacencyList(num_thread, transaction_size, (test_size * transaction_size * 2));
    t_data = new ThreadData[num_thread];

    printf("Starting test...\n\n");
    
    //Create all our threads as joinable
    pthread_t thread[num_thread];
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    void *status;
    intptr_t i;

    clock_gettime(CLOCK_MONOTONIC, &start);

    //Create the threads and send them into the function
    for (i = 0; i < num_thread; i++)
    {
        pthread_create(&thread[i], &attr, &listTest, (void *)i);
    }

    //Join threads
    pthread_attr_destroy(&attr);
    for (i = 0; i < num_thread; i++)
    {
        pthread_join(thread[i], &status);
    }

    clock_gettime(CLOCK_MONOTONIC, &finish);

    elapsed = (finish.tv_sec - start.tv_sec);
    elapsed += (finish.tv_nsec - start.tv_nsec) / (double)1000000000.0;

    int g_commits = 0;
    int g_aborts = 0;

    for (i = 0; i < num_thread; i++)
    {
        g_commits += t_data[i].g_commits;
        g_aborts += t_data[i].g_aborts;
    }

    printf("Ops/s %.0f\n", (g_commits*transaction_size)/elapsed);
    printf("Total Commits %d, Total Aborts: %d \n", g_commits, g_aborts);
    printf("Success Rate: %f%% \n", 100*((double)g_commits/(test_size*transaction_size)));
}