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
    vector<vector<int>> array_part;
public:
    int rank;
    int N;
    int M;
    int N1;
    int rank_count;

    static string get_func(Executor *executor, string sub_op, int row);

    string execute_command(const string &command);

    static int parse_target_row(const string &command);

    Executor(int current_rank, int rowN, int colM, int partRowN, int rankCount) :
            op_map{
                    {"get", {get_func, {"row", "aggr"}}}
            },
            rank(current_rank),
            rank_count(rankCount),
            N(rowN),
            M(colM),
            N1(partRowN),
            // allocate array N1 x M;
            array_part(partRowN, vector<int>(colM, current_rank)) {
    }
};


#endif //MPI_TEST_EXECUTOR_H
