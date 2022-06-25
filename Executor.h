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
    map<string, map<string,
            string (*)(Executor *, int)>> op_map{{
                                                         "get", {{"row", get_row},
                                                                 {"aggr", get_aggr}}
                                                 }};
    vector<vector<int>> array_part;
    map<string, string (*)(Executor *)> special_op_map{
            {"exit", exit}
    };

    static string exit(Executor *executor);

public:
    int rank;
    int N;
    int M;
    int N1;
    int rank_count;

    static string get_row(Executor *executor, int row);

    static string get_aggr(Executor *executor, int row);

    string execute_command(const string &command);

    static int parse_target_row(const string &command);

    int get_local_row(int row) const;

    Executor(int current_rank, int rowN, int colM, int partRowN, int rankCount) :
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
