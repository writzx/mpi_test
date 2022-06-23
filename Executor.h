//
// Created by Writwick Das on 21/06/2022.
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
    map<string, pair<string (*)(Executor *, string, int), vector<string>>> op_map;
public:
    int N;
    int M;
    int N1;
    int rank_count;

    static string get_func(Executor *executor, string sub_op, int row);

    void execute_command(const string &command);
    int parse_target_row(const string &command);

    Executor(int rowN, int colM, int partRowN, int rankCount) : op_map{
            {"get", {get_func, {"row", "aggr"}}}
    } {
        rank_count = rankCount;
        N = rowN;
        M = colM;
        N1 = partRowN;
    }
};


#endif //MPI_TEST_EXECUTOR_H
