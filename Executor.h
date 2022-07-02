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
            int *(*)(Executor *, int, int &)>> op_map{{
                                                              "get", {{"row", get_row},
                                                                      {"aggr", get_aggr}}
                                                      }};
    map<string, map<string,
            int *(*)(Executor *, int, int, int &)>> range_op_map{{
                                                                         "get", {{"aggr", get_aggr_range}}
                                                                 }};
    // holds special functions with only command name (no arguments)
    static map<string, int *(*)(Executor *, int &)> special_op_map;

    // holds the allocated array
    vector<vector<int>> array_part;

    static int *exit(Executor *executor, int &);

public:
    int rank;
    int N;
    int M;
    int N1;
    int rank_count;

    static int *get_row(Executor *executor, int row, int &count);

    static int *get_aggr(Executor *executor, int row, int &count);

    static int *get_aggr_range(Executor *executor, int row_start, int row_end, int &count);

    int *execute_command(string command, int &count);

    int parse_command(const string &command, map<int, pair<int, int>> &sub_command_map) const;

//    int get_local_row(int row) const;

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
