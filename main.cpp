#include <mpi.h>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <future>
#include <cmath>

#include "Executor.h"

using namespace std;

Executor *executor;

vector<string> read_commands(const string &path);

void mpi_loop();

void validate_and_execute(const string &command, int N, int N1, int rank_count);

int main(int argc, char **argv) {
    if (argc < 2) {
        cout << "error: total number of rows not specified." << endl;
        return 0;
    }

    if (argc < 3) {
        cout << "error: total number of cols not specified." << endl;
        return 0;
    }

    // try to register for multi-thread mpi
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    if (provided != MPI_THREAD_MULTIPLE) {
        // if failed to register for multi-threading, show an error message and quit
        cout << "error: couldn't register mpi for multi-thread operations." << endl;
        MPI_Finalize();
        return 0;
    }

    int total_rank; // #n
    MPI_Comm_size(MPI_COMM_WORLD, &total_rank);

    int current_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &current_rank);

    // try to parse N from command line
    stringstream bigN_str(argv[1]);
    int bigN(0);
    bigN_str >> bigN;

    if (bigN_str.fail()) {
        cout << "error: invalid value for N." << endl;
        return 0;
    }

    // try to parse M from command line
    stringstream bigM_str(argv[2]);
    int bigM(0);
    bigM_str >> bigM;

    if (bigM_str.fail()) {
        cout << "error: invalid value for M." << endl;
        return 0;
    }

    // calculate N1 (average rows per rank)
    int N1 = ceil((float) bigN / total_rank);

    // allocate only remaining rows if it's the last rank
    if (current_rank == total_rank - 1) {
        N1 = bigN - (total_rank - 1) * N1;
    }

    // initialize executor for all ranks including 0
    executor = new Executor(current_rank, bigN, bigM, N1, total_rank);

    // start the mpi loop asynchronously for all ranks including 0
    auto task = async(mpi_loop);

    // cout << "rank " << current_rank << " pid: " << getpid() << endl;

    // only allow running commands if rank is 0
    if (current_rank == 0) {
        if (argc < 4) {
            string command;
            do {
                // read commands and execute them until "exit" is called
                getline(cin, command);
                validate_and_execute(command, bigN, N1, total_rank);
            } while (command != "exit");
        } else {
            auto commands = read_commands(argv[3]);
            bool exited = false;

            // read commands and execute until end of file or until we reach an "exit" call
            for (const string &command: commands) {
                validate_and_execute(command, bigN, N1, total_rank);
                if (command.substr(0, 4) == "exit") {
                    exited = true;
                    break;
                }
            }

            if (!exited) {
                // if exit was not called, call it once
                validate_and_execute("exit", bigN, N1, total_rank);
            }
        }
    }

    // wait for mpi loop to exit
    task.wait();

    MPI_Finalize();

    return 0;
}

// executes remote commands and prints their results
vector<int> execute_remote_command(int rank, const string &command) {
    // send the command to the target rank
    cout << "rank " << rank << " << " << command << endl;
    MPI_Send(command.c_str(), command.length(), MPI_CHAR, rank, 0, MPI_COMM_WORLD);

    int result_len;
    MPI_Status status;

    // receive the result length then receive the result value and print it
    MPI_Probe(rank, 0, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_INT, &result_len);


    vector<int> result;
    result.resize(result_len);

    MPI_Recv(&result[0], result_len, MPI_INT, rank, 0,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    return result;
}

string generate_sub_command(pair<int, int> sub_comm_element, string command_prefix) {
    stringstream sub_command_stream;
    const int row_start = sub_comm_element.first;
    const int row_end = sub_comm_element.second;
    sub_command_stream << command_prefix << row_start;
    if (row_end > row_start) {
        sub_command_stream << "-" << row_end;
    }

    return sub_command_stream.str();
}

string format_array(const vector<int> &array) {
    stringstream res_stream;

    // format as an array e.g.: { 1, 2, ... }
    string sep = "{ ";
    for (const auto &r: array) {
        res_stream << sep << r;
        sep = ", ";
    }

    res_stream << " }";

    return res_stream.str();
}

// validates commands then executes them
void validate_and_execute(const string &command, int N, int N1, int rank_count) {
    map<int, pair<int, int>> sub_command_map;
    string command_prefix;

    P_RESULT parse_result = executor->parse_command(command, sub_command_map, command_prefix);

    // operator is empty, ignore
    if (parse_result == EMPTY_OP) {
        return;
    }

    // special operator
    // it should be sent to all ranks
    if (parse_result == SPECIAL_OPERATOR) {
        for (int i = 0; i < rank_count; ++i) {
            execute_remote_command(i, command);
        }
        return;
    }

    // otherwise just show error message
    if (parse_result == ERROR_OPERATOR) {
        cout << "error: invalid command or arguments." << endl;
        return;
    }

    if (parse_result == ROW_OUT_OF_RANGE) {
        // row is out of range
        // get the rank values from the special map element
        auto sp_map_row_ele = sub_command_map.find(-1);
        auto sp_map_row_end_ele = sub_command_map.find(-2);
        cout << "error: invalid row input: " << sp_map_row_ele->second.second;
        if (sp_map_row_end_ele != sub_command_map.end()) {
            cout << "-" << sp_map_row_end_ele->second.second;
        }
        cout << " (inferred rank: " << sp_map_row_ele->second.first;
        if (sp_map_row_end_ele != sub_command_map.end()) {
            cout << "-" << sp_map_row_end_ele->second.first;
        }
        cout << " is invalid. valid row value range: [0, "
             << N - 1 << "])" << endl;
        return;
    }

    if (parse_result == NEGATIVE_ROW_RANGE) {
        // end row is greater than start row
        // get the rank values from the special map element
        auto sp_map_row_ele = sub_command_map.find(-1);
        cout << "error: invalid row range input. end row(" << sp_map_row_ele->second.second
             << ") cannot be greater than start row(" << sp_map_row_ele->second.first << ")." << endl;
        return;
    }

    if (sub_command_map.size() == 1) {
        const auto sub_comm = sub_command_map.begin();
        const int rank = sub_comm->first;
        auto result = execute_remote_command(rank, generate_sub_command(
                sub_comm->second, command_prefix));
        if (result.size() == 1) {
            // check if the result length is 1, print as single int value
            if (result[0] > -1) {
                // if result value is -1 or lower, it means error so don't print
                // anything since the error is already printed by the other rank
                cout << "rank " << rank << " >> " << result[0] << endl;
            }
        } else {
            // print the formatted array as output
            cout << "rank " << rank << " >> " << format_array(result) << endl;
        }
        return;
    }
    vector<future<vector<int>>> future_results;

    for (auto &sub_comm: sub_command_map) {
        const int rank = sub_comm.first;

        future_results.push_back(async(execute_remote_command, rank, generate_sub_command(
                sub_comm.second, command_prefix)));
    }

    vector<vector<int>> accumulator;

    for (auto &future_result: future_results) {
        auto res = future_result.get();

        accumulator.push_back(res);
    }

    if (accumulator[0].size() == 1) {
        // only one value is returned per rank
        // so aggregate them and print
        int aggr = 0;
        for (auto const &v: accumulator) {
            if (v[0] > 0) {
                aggr += v[0];
            }
        }
        cout << "aggregate result: " << aggr << endl;
    } else {
        string prefix = "range result: ";
        for (const auto &row: accumulator) {
            // print the formatted array as output
            cout << prefix << format_array(row) << endl;
            prefix = "              ";
        }
    }
}

// the basic mpi loop for receiving and executing commands then sending back results
void mpi_loop() {
    string command;
    do {
        int command_len;
        MPI_Status status;

        // receive the command length then receive the command
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &command_len);

        command.resize(command_len);

        MPI_Recv(&command[0], command_len, MPI_CHAR, 0, 0,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // run the command send the result back
        int count;
        auto result = executor->execute_command(command, count);

        MPI_Send(result, count, MPI_INT, 0, 0, MPI_COMM_WORLD);
    } while (command.substr(0, 4) != "exit");
}

// reads all commands from file
vector<string> read_commands(const string &path) {
    ifstream file_stream(path);
    string command;
    vector<string> commands;

    while (getline(file_stream, command)) {
        commands.push_back(command);
    }

    return commands;
}
