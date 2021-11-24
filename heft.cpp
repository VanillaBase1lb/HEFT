#include <iostream>
#include <vector>
#include <fstream>
#include <iomanip>
#include <numeric>
#include <algorithm>
#define MAX_TIME 10000

using namespace std;

class Processor {
public:
    int computation_cost;
    int est;
    int eft;
};

class Job {
public:
    float computaion_avg; // avg. computation cost across all processors
    float rank;
    int processor_exec; // the processor on which the job was finally scheduled
    int st; // start time in final schedule
    int ft; // finish time in final schedule
    vector<Processor> processors;
    vector<int> parents; // indexes of all the parents of a particular node
};

vector<Job> jobs;
int job_count; // number of jobs
int processor_count; // number of processors
vector<vector<int>> communication_cost_dag; // communication cost DAG in adjacency matrix form

// initializes all the data from input.txt
void initializeData() {
    ifstream num("input.txt");
    int temp;

    // read number of jobs and processors
    num >> job_count;
    num >> processor_count;
    // fill the vectors with empty data
    for (int i = 0; i < job_count; i++) {
        Job *jb = new Job;
        jobs.push_back(*jb);
        for (int j = 0; j < processor_count; j++) {
            Processor *p = new Processor;
            jobs[i].processors.push_back(*p);
        }
    }

    // fill the computation costs of all jobs on all processors
    for (int i = 0; i < processor_count; i++) {
        for (int j = 0; j < job_count; j++) {
            num >> temp;
            jobs[j].processors[i].computation_cost = temp;
        }
    }
    
    // fill the communications directed acyclic graph adjacency matrix
    for (int i = 0; i < job_count; i++) {
        vector<int> v(job_count);
        communication_cost_dag.push_back(v);
        for (int j = 0; j < job_count; j++) {
            num >> temp;
            communication_cost_dag[i][j] = temp;
            if (communication_cost_dag[i][j] > 0) {
                jobs[j].parents.push_back(i);
            }
        }
    }
}
 
// calculate average computation costs of all jobs
void computationAvgCalculate() {
    for (int i = 0; i < job_count; i++) {
        jobs[i].computaion_avg = 0;
        for (int j = 0; j < processor_count; j++) {
            jobs[i].computaion_avg += jobs[i].processors[j].computation_cost;
        }
        jobs[i].computaion_avg = jobs[i].computaion_avg / processor_count;
    }
}

// for every node, recursively find the upper rank
float rankCalculate(int node) {
    float sub_rank = 0; // rank of the dependent task
    float temp_max = 0; // temp_max = maximum of(cost of communication between jobs + rank(i))

    for (int i = 0; i < job_count; i++) { // for every node
        if (communication_cost_dag[node][i] > 0) { // that has an incoming edge from main node
            sub_rank = rankCalculate(i); // calculate its upper rank
            if (sub_rank + communication_cost_dag[node][i] > temp_max) { // if the upper rank is max among others
                temp_max = sub_rank + communication_cost_dag[node][i]; // set this as the max
            }
        }
    }
    
    return temp_max + jobs[node].computaion_avg; // rank of the main node
}

// sort the jobs w.r.t upper ranks and return in a vector
vector<int> sortRank() {
    vector<int> index(job_count);
    iota(index.begin(), index.end(), 0);
    sort(index.begin(), index.end(), [](int a, int b) -> bool {
        return jobs[a].rank > jobs[b].rank;
    });
    
    return index;
}

// searches for gaps in between jobs on a processor and returns where the insertion should be
int jobInsertion(int search_end, vector<vector<bool>> &processor_state, int job_index, int processor_index, int search_start) {
    int t = jobs[job_index].processors[processor_index].computation_cost;
    int counter = 0;
    for (int i = search_start; i < search_end; i++) {
        if (processor_state[processor_index][i] == false) {
            counter++;
        }
        else {
            counter = 0;
        }
        if (counter == t) { // as soon as a compatible gap is found, return its location
            return i;
        }
    }
    return max(search_end, search_start); // if no gaps were found, resume the scheduling as normal
}

// calculate earliest start and finish times of all jobs
void schedule(vector<int> rank_index_sorted) {
    vector<int> processor_free(processor_count, 0); // time at which the processor will be free after its latest job
    int processor_current = -1; // same as job[ii].processor_exec, just for code simplicity
    vector<int> parent_job_delay(job_count, 0); // delay added to processor_free, accounting for finish times of all parent nodes and communication costs
    vector<vector<bool>> processor_state(processor_count, vector<bool>(MAX_TIME, false)); // defines the state of processors at each time unit, max time limit is MAX_TIME

    // scheduling jobs
    for (int i = 0; i < job_count; i++) {
        int ii = rank_index_sorted[i]; // for making the code simpler
        vector<vector<int>> processor_ready(processor_count); // time when the processor is ready, including communication cost but stores the data for all parent nodes

        for (int j = 0; j < processor_count; j++) { // for each processor w.r.t each job
            for (int k = 0; k < jobs[ii].parents.size(); k++) { // for each parent node of the job calculate ready times
                if (jobs[jobs[ii].parents[k]].processor_exec != j) // if processor different from the processor parent was executed on, add communication cost delay
                    processor_ready[j].push_back(communication_cost_dag[jobs[ii].parents[k]][ii] + jobs[jobs[ii].parents[k]].ft);
                else // if on same processor, ignore communication cost
                    processor_ready[j].push_back(jobs[jobs[ii].parents[k]].ft);
            }
            if (processor_ready[j].size() > 0) // final delay on the processor for each job, this considers everythin
                parent_job_delay[j] = *max_element(processor_ready[j].begin(), processor_ready[j].end());
            else
                parent_job_delay[j] = 0;
            jobs[ii].processors[j].est = jobInsertion(processor_free[j], processor_state, ii, j, parent_job_delay[j]);
            jobs[ii].processors[j].eft = jobs[ii].processors[j].est + jobs[ii].processors[j].computation_cost;
        }

        processor_current = min_element(jobs[ii].processors.begin(), jobs[ii].processors.end(), [](Processor &a, Processor &b) -> bool {
            return a.eft < b.eft;
        }) - jobs[ii].processors.begin(); // min_element returns reference to the min element in vector, substracting it with reference of first element gives the index of minimum element

        jobs[ii].st = jobs[ii].processors[processor_current].est;
        jobs[ii].ft = jobs[ii].processors[processor_current].eft;
        jobs[ii].processor_exec = processor_current;
        processor_free[processor_current] = jobs[ii].ft;

        for (int j = jobs[ii].st; j < jobs[ii].ft; j++) { // update the state of processor to reflect that processor was in use for the entire duration of job execution
            processor_state[processor_current][j] = true;
        }
    }
}

int main(int argc, char *argv[]) {
    initializeData();

    cout << "No. of tasks:" << job_count << endl;
    cout << "No. of processors:" << processor_count << endl;
    cout << "\nThe upward rank values:" << endl;

    computationAvgCalculate();

    for (int i = 0; i < job_count; i++) {
        jobs[i].rank = rankCalculate(i);
        cout << "Task " << i + 1 << ": " <<  fixed << setprecision(6) << jobs[i].rank << endl;
    }

    vector<int> rank_index_sorted = sortRank();
    cout << "\nThe order of tasks to be scheduled:" << endl;
    for (int i = 0; i < job_count; i++) {
        cout << rank_index_sorted[i] + 1 << " ";
    }

    schedule(rank_index_sorted);

    cout << "\n\nEST and EFT on different processors" << endl;
    for (int i = 0; i < job_count; i++) {
        cout << "Task: " << i + 1 << endl;
        for (int j = 0; j < processor_count; j++) {
            cout << "processor " << j + 1 << "||est: " << jobs[i].processors[j].est << " eft: " << jobs[i].processors[j].eft << " ||" << endl;
        }
        cout << endl;
    }
    
    cout << "\nFinal Schedule:" << endl;
    for (int i = 0; i < job_count; i++) {
        cout << "Task " << i + 1 << " is executed on processor " << jobs[i].processor_exec + 1 << " from time " << jobs[i].st << " to " << jobs[i].ft << endl;
    }
    
    int schedule_length; // schedule length has to be calculated since a job with higher upper rank can finish after the lowest rank job
    schedule_length = jobs[max_element(jobs.begin(), jobs.end(), [](Job &a, Job &b) -> bool {
        return a.ft < b.ft;
    }) - jobs.begin()].ft;
    cout << "\nHence, the makespan length from the schedule: " << schedule_length << endl;

    return 0;
}
