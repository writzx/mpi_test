//
// Created by arik on 21/06/2022.
//

#ifndef MPI_TEST_EXECUTOR_H
#define MPI_TEST_EXECUTOR_H

#include <iostream>
#include <vector>
#include <map>

using namespace std;

class Executor {
private:
    // holds the operators, functions and sub operators
    map<string, pair<void (*)(Executor *, string, int), vector<string>>> op_map;
public:
    int total_rows;
    int total_ranks;

    static void get_func(Executor *executor, string sub_op, int row);

    void execute_command(const string &command);

    Executor(int rowN, int rankN) : op_map{
            {"get", {get_func, {"row", "aggr"}}}
    } {
        total_ranks = rankN;
        total_rows = rowN;
    }
};


#endif //MPI_TEST_EXECUTOR_H
